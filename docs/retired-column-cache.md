# Retired column cache policy

A column can override `pin-index-and-filter-blocks` (default true) and
`disable-auto-compactions` (null inherits the global policy). For a retained,
frozen column, use a small named cache, disable index/filter pinning, and disable
automatic compactions. Other columns retain their own/default policy.

Production messages-v1 retirement uses a 16 MiB named cache, pinning disabled,
and automatic compactions disabled. Messages-v2 uses the 192 GiB default cache
(plus the existing 3 GiB write-buffer-manager accounting); the sender index keeps
40 GiB. DB table readers are bounded at 8192 and eager opening is disabled.

A small LRU capacity alone is not a strict bound on pinned entries. Live telemetry
showed about 3 GiB pinned despite the retired column's 16 MiB configured capacity;
this is why pinning must be disabled as well. Retained MANIFEST/column metadata
still consumes some memory. No column or data file is deleted by this policy.
