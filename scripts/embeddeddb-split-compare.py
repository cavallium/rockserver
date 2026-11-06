#!/usr/bin/env python3
"""Checks that the EmbeddedDB split moved code without rewriting it.

Compares the trimmed, sorted lines of EmbeddedDB.java at a commit before the split (default 60c9321) with the
lines of the current EmbeddedDB*.java files. Level 0 is the raw comparison. Level 1 ignores imports, the
call-site prefixes the move added (edb., writes., EmbeddedDBRanges., ...), `this.` and a leading private/public;
it must find no line of the old file missing except the one known edit (EmbeddedDB.varHandle). The lines it
lists as new are the parts' headers and the facade's one-line delegates.

Usage: scripts/embeddeddb-split-compare.py [repo root] [base commit]

When the split is merged with branches that still edited the unsplit EmbeddedDB.java, re-apply their edits in the
parts and pass the merge's first parent (the unsplit file with those edits) as the base commit: level 1 must then
again miss only the known edit.
"""
import re, sys, subprocess, glob, collections
W = sys.argv[1] if len(sys.argv) > 1 else '.'
BASE = sys.argv[2] if len(sys.argv) > 2 else '60c9321' 
P = W + '/src/main/java/it/cavallium/rockserver/core/impl/'
old = subprocess.check_output(['git', '-C', W, 'show', BASE + ':src/main/java/it/cavallium/rockserver/core/impl/EmbeddedDB.java'], text=True)
new = ''.join(open(f).read() + '\n' for f in sorted(glob.glob(P + 'EmbeddedDB*.java')))
# The one line the split had to edit (see design.md): varHandle takes a private lookup in the owner class.
KNOWN_EDITS = {'return MethodHandles.lookup().findVarHandle(owner, field, type);'}
fields = 'columnOps|storageMetrics|writeElision|writes|batchWrites|pointReads|existsMultiOps|iterators|ranges|rawScan|maintenance'
def norm(text, level):
    out = []
    for l in text.split('\n'):
        t = l.strip()
        if not t or t.startswith('import '): continue  # unused imports were dropped; imports are not code
        if level >= 1:
            t = re.sub(r'\bedb\.(' + fields + r')\.', '', t)
            t = re.sub(r'\b(' + fields + r')\.(new )?(?=\w)', r'\2', t)
            t = re.sub(r'\b(' + fields + r')::', 'this::', t)
            t = re.sub(r'\bedb\.new ', 'new ', t)
            t = re.sub(r'\bedb\.', '', t)
            t = re.sub(r'\bEmbeddedDB\w*\.(?=[\w<])', '', t)
            t = re.sub(r'\bEmbeddedDB\w*::', 'EmbeddedDB::', t)
            t = re.sub(r'\bEmbeddedDB\.this\b', 'edb', t)
            t = re.sub(r'\bthis\.(?=\w)', '', t)
            t = re.sub(r'^(private|public) ', '', t)
            t = re.sub(r'^(private|public) ', '', t)
        out.append(t)
    return collections.Counter(out)
for level in (0, 1):
    a, b = norm(old, level), norm(new, level)
    removed = a - b; added = b - a
    print(f'== level {level}: old lines {sum(a.values())}, new lines {sum(b.values())}, only-old {sum(removed.values())}, only-new {sum(added.values())}')
    if level == 1:
        for k, v in sorted(removed.items()): print('-', v, k[:200])
        for k, v in sorted(added.items()):
            print('+', v, k[:200])
    if level == 1 and set(removed) - KNOWN_EDITS:
        sys.exit(1)
