#!/usr/bin/env python3
"""Reproduce only the qualified Java25 legacy instrumentation overlay; never deploys."""
import argparse
import copy
import hashlib
import json
import pathlib
import zipfile


def digest(data):
    return hashlib.sha256(data).hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--original', type=pathlib.Path, required=True)
    parser.add_argument('--instrumented-classes', type=pathlib.Path, required=True)
    parser.add_argument('--cutoff-classes', type=pathlib.Path, required=True)
    parser.add_argument('--output', type=pathlib.Path, required=True)
    args = parser.parse_args()
    manifest = json.loads(pathlib.Path(__file__).with_name('manifest.json').read_text())
    require(digest(args.original.read_bytes()) == manifest['original_sha256'], 'wrong original JAR')
    require(not args.output.exists() and not args.output.is_symlink(), 'output must be new')
    payloads = {}
    for entry in manifest['entries']:
        root = args.cutoff_classes if entry['source'] == 'cutoff' else args.instrumented_classes
        data = (root / entry['name']).read_bytes()
        require(digest(data) == entry['sha256'], 'unqualified class: ' + entry['name'])
        require(data[:4] == b'\xca\xfe\xba\xbe' and int.from_bytes(data[6:8], 'big') == 69,
                'Java25 class required: ' + entry['name'])
        payloads[entry['name']] = data
    changed = {e['name'] for e in manifest['entries'] if e['kind'] == 'replace'}
    added = [e for e in manifest['entries'] if e['kind'] == 'add']
    created = False
    try:
        with zipfile.ZipFile(args.original) as original:
            names = original.namelist()
            require(len(names) == len(set(names)) == manifest['original_entry_count'], 'original inventory mismatch')
            require(not any(n.upper().endswith(('.SF', '.RSA', '.DSA')) for n in names), 'signed JAR refused')
            for entry in manifest['entries']:
                if entry['kind'] == 'replace':
                    require(digest(original.read(entry['name'])) == entry['original_sha256'], 'original class mismatch')
                else:
                    require(entry['name'] not in names, 'added class already exists')
            with args.output.open('xb') as output:
                created = True
                with zipfile.ZipFile(output, 'w', zipfile.ZIP_DEFLATED) as result:
                    for info in original.infolist():
                        result.writestr(copy.copy(info), payloads[info.filename] if info.filename in changed else original.read(info.filename))
                    for entry in added:
                        info = zipfile.ZipInfo(entry['name'], tuple(entry['zip']['date_time']))
                        for key, value in entry['zip'].items():
                            if key != 'date_time':
                                setattr(info, key, bytes.fromhex(value) if key in ('extra', 'comment') else value)
                        result.writestr(info, payloads[entry['name']])
            with zipfile.ZipFile(args.output) as result:
                require(result.testzip() is None, 'ZIP corruption')
                require(set(result.namelist()) == set(names) | {e['name'] for e in added}, 'output inventory mismatch')
                actual = {n for n in names if original.read(n) != result.read(n)}
                require(actual == changed, 'binary whitelist mismatch')
                require(all(result.read(e['name']) == payloads[e['name']] for e in manifest['entries']), 'payload mismatch')
        actual_sha = digest(args.output.read_bytes())
        require(actual_sha == manifest['qualified_overlay_sha256'], 'archive SHA mismatch; do not deploy this reproduction')
        print(json.dumps({'sha256': actual_sha, 'replaced': len(changed), 'added': len(added),
                          'all_other_entry_payloads_unchanged': True, 'deployment_performed': False}))
    except BaseException:
        if created:
            args.output.unlink(missing_ok=True)
        raise


if __name__ == '__main__':
    main()
