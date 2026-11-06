/*
 * Tests only (RT-23, RT-64): an LD_PRELOAD shim that makes the storage of one node fail, without root.
 *
 * It interposes the libc calls that write or sync a file: write, pwrite, writev, pwritev (and their 64-bit and v2
 * variants), fsync, fdatasync, sync_file_range, fallocate and posix_fallocate. While the trigger file exists, a call
 * on a file descriptor whose path lies under the faulty directory fails with the error the trigger names, and
 * changes nothing; every other call goes to libc unchanged. The JVM's file channels, RocksDB (a JNI library that
 * calls libc through the PLT) and Ratis' log all reach these functions.
 *
 * Environment:
 *   FAULTFS_DIR      the faulty directory; without it the shim does nothing. It is compared with the canonical path of
 *                    each file descriptor (/proc/self/fd), so it is resolved with realpath: at load time, or, if it did
 *                    not exist yet (a node's first start creates its data directory), at the first faulted call after
 *                    it appeared. Until then the path is compared as given.
 *   FAULTFS_TRIGGER  the trigger file; it should lie outside FAULTFS_DIR
 *
 * Trigger file contents: "<ERRNO> [<OPS>]", for example "EIO", "ENOSPC write" or "EIO sync".
 *   ERRNO  EIO (default), ENOSPC, EDQUOT or EROFS
 *   OPS    all (default): every interposed call; write: the write and fallocate calls only (a full disk);
 *          sync: fsync, fdatasync and sync_file_range only (a disk that fails on flush)
 * The trigger is read at most once every 2 ms per process, so it takes effect within a few milliseconds.
 *
 * What it does not do: fail open, mkdir, rename or unlink (a full disk also refuses new files: RocksDB and Ratis
 * write to a new file before they rename it, so the write fails first), writes through mmap (neither RocksDB's
 * write path nor Ratis' log use it), io_uring, and direct syscall() calls.
 *
 * Build: gcc -shared -fPIC -O2 -o libfaultfs.so faultfs.c -ldl -lpthread
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>

#define OP_WRITE 1
#define OP_SYNC 2

/* The path as given, and its realpath once it resolved (resolved_dir is written once, before resolved is set). */
static char given_dir[PATH_MAX];
static size_t given_dir_len;
static char resolved_dir[PATH_MAX];
static size_t resolved_dir_len;
static int resolved;
static pthread_mutex_t resolve_lock = PTHREAD_MUTEX_INITIALIZER;
static char trigger_path[PATH_MAX];
static int enabled;

/* The trigger as last read: errno to return (0: no fault) and the operations it applies to. Races are benign. */
static volatile int current_errno;
static volatile int current_ops;
static volatile long long next_check_ns;

static size_t trim_slashes(char *path) {
	size_t len = strlen(path);
	while (len > 1 && path[len - 1] == '/') {
		path[--len] = '\0';
	}
	return len;
}

/* Tries realpath on the given directory; after a success the resolved path is used from then on. */
static void try_resolve(void) {
	if (__atomic_load_n(&resolved, __ATOMIC_ACQUIRE)) {
		return;
	}
	int saved = errno;
	pthread_mutex_lock(&resolve_lock);
	if (!resolved) {
		char buffer[PATH_MAX];
		if (realpath(given_dir, buffer) != NULL) {
			strcpy(resolved_dir, buffer);
			resolved_dir_len = trim_slashes(resolved_dir);
			__atomic_store_n(&resolved, 1, __ATOMIC_RELEASE);
		}
	}
	pthread_mutex_unlock(&resolve_lock);
	errno = saved;
}

__attribute__((constructor)) static void faultfs_init(void) {
	const char *dir = getenv("FAULTFS_DIR");
	const char *trigger = getenv("FAULTFS_TRIGGER");
	if (dir == NULL || trigger == NULL || strlen(trigger) >= sizeof(trigger_path) || strlen(dir) >= sizeof(given_dir)) {
		return;
	}
	strcpy(given_dir, dir);
	given_dir_len = trim_slashes(given_dir);
	strcpy(trigger_path, trigger);
	enabled = 1;
	try_resolve();
}

static long long now_ns(void) {
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (long long) ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

static int parse_errno(const char *word) {
	if (strcmp(word, "ENOSPC") == 0) return ENOSPC;
	if (strcmp(word, "EDQUOT") == 0) return EDQUOT;
	if (strcmp(word, "EROFS") == 0) return EROFS;
	return EIO;
}

/* Reads the trigger file with calls this shim does not interpose (open, read, close). */
static void refresh_trigger(void) {
	long long now = now_ns();
	if (now < next_check_ns) {
		return;
	}
	int saved = errno;
	int fd = open(trigger_path, O_RDONLY | O_CLOEXEC);
	if (fd < 0) {
		current_errno = 0;
	} else {
		char buffer[64];
		ssize_t n = read(fd, buffer, sizeof(buffer) - 1);
		close(fd);
		buffer[n > 0 ? n : 0] = '\0';
		char first[32] = "EIO";
		char second[32] = "all";
		sscanf(buffer, "%31s %31s", first, second);
		current_ops = strcmp(second, "write") == 0 ? OP_WRITE
				: strcmp(second, "sync") == 0 ? OP_SYNC : OP_WRITE | OP_SYNC;
		current_errno = parse_errno(first);
	}
	next_check_ns = now + 2000000LL;
	errno = saved;
}

/* Whether a call of kind op on fd must fail now; sets errno if so. */
static int must_fail(int fd, int op) {
	if (!enabled) {
		return 0;
	}
	refresh_trigger();
	int error = current_errno;
	if (error == 0 || (current_ops & op) == 0) {
		return 0;
	}
	try_resolve();
	const char *fault_dir = given_dir;
	size_t fault_dir_len = given_dir_len;
	if (__atomic_load_n(&resolved, __ATOMIC_ACQUIRE)) {
		fault_dir = resolved_dir;
		fault_dir_len = resolved_dir_len;
	}
	int saved = errno;
	char link[64];
	char path[PATH_MAX];
	snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
	ssize_t n = readlink(link, path, sizeof(path) - 1);
	errno = saved;
	if (n <= 0) {
		return 0;
	}
	path[n] = '\0';
	if ((size_t) n < fault_dir_len || strncmp(path, fault_dir, fault_dir_len) != 0
			|| (path[fault_dir_len] != '\0' && path[fault_dir_len] != '/')) {
		return 0;
	}
	errno = error;
	return 1;
}

#define REAL(name) \
	static __typeof__(name) *real_##name; \
	if (real_##name == NULL) { real_##name = (__typeof__(name) *) dlsym(RTLD_NEXT, #name); }

ssize_t write(int fd, const void *buf, size_t count) {
	REAL(write);
	if (must_fail(fd, OP_WRITE)) return -1;
	return real_write(fd, buf, count);
}

ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset) {
	REAL(pwrite);
	if (must_fail(fd, OP_WRITE)) return -1;
	return real_pwrite(fd, buf, count, offset);
}

ssize_t pwrite64(int fd, const void *buf, size_t count, off64_t offset) {
	REAL(pwrite64);
	if (must_fail(fd, OP_WRITE)) return -1;
	return real_pwrite64(fd, buf, count, offset);
}

ssize_t writev(int fd, const struct iovec *iov, int iovcnt) {
	REAL(writev);
	if (must_fail(fd, OP_WRITE)) return -1;
	return real_writev(fd, iov, iovcnt);
}

ssize_t pwritev(int fd, const struct iovec *iov, int iovcnt, off_t offset) {
	REAL(pwritev);
	if (must_fail(fd, OP_WRITE)) return -1;
	return real_pwritev(fd, iov, iovcnt, offset);
}

ssize_t pwritev64(int fd, const struct iovec *iov, int iovcnt, off64_t offset) {
	REAL(pwritev64);
	if (must_fail(fd, OP_WRITE)) return -1;
	return real_pwritev64(fd, iov, iovcnt, offset);
}

ssize_t pwritev2(int fd, const struct iovec *iov, int iovcnt, off_t offset, int flags) {
	REAL(pwritev2);
	if (must_fail(fd, OP_WRITE)) return -1;
	return real_pwritev2(fd, iov, iovcnt, offset, flags);
}

int fsync(int fd) {
	REAL(fsync);
	if (must_fail(fd, OP_SYNC)) return -1;
	return real_fsync(fd);
}

int fdatasync(int fd) {
	REAL(fdatasync);
	if (must_fail(fd, OP_SYNC)) return -1;
	return real_fdatasync(fd);
}

int sync_file_range(int fd, off64_t offset, off64_t nbytes, unsigned int flags) {
	REAL(sync_file_range);
	if (must_fail(fd, OP_SYNC)) return -1;
	return real_sync_file_range(fd, offset, nbytes, flags);
}

int fallocate(int fd, int mode, off_t offset, off_t len) {
	REAL(fallocate);
	if (must_fail(fd, OP_WRITE)) return -1;
	return real_fallocate(fd, mode, offset, len);
}

int fallocate64(int fd, int mode, off64_t offset, off64_t len) {
	REAL(fallocate64);
	if (must_fail(fd, OP_WRITE)) return -1;
	return real_fallocate64(fd, mode, offset, len);
}

/* These return the error number instead of setting errno, as libc does. */
int posix_fallocate(int fd, off_t offset, off_t len) {
	REAL(posix_fallocate);
	int saved = errno;
	if (must_fail(fd, OP_WRITE)) {
		int error = errno;
		errno = saved;
		return error;
	}
	return real_posix_fallocate(fd, offset, len);
}

int posix_fallocate64(int fd, off64_t offset, off64_t len) {
	REAL(posix_fallocate64);
	int saved = errno;
	if (must_fail(fd, OP_WRITE)) {
		int error = errno;
		errno = saved;
		return error;
	}
	return real_posix_fallocate64(fd, offset, len);
}
