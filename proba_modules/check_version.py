#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai


__license__ = 'GPL v3'
__copyright__ = '2009, Kovid Goyal <kovid@kovidgoyal.net>'
__docformat__ = 'restructuredtext en'

import os
import re
import sys

src_base = os.path.dirname(os.path.abspath(__file__))


def check_version_info():
    print()
    print('src_base = ', src_base)
    print()
    with open(os.path.join(src_base, 'pyproject.toml')) as f:
        raw = f.read()
    m = re.search(r'''^requires-python\s*=\s*['"](.+?)['"]''', raw, flags=re.MULTILINE)
    print('m = ', m)
    print()
    assert m is not None
    minver = m.group(1)
    m = re.match(r'(>=?)(\d+)\.(\d+)', minver)
    q = int(m.group(2)), int(m.group(3))
    if m.group(1) == '>=':
        is_ok = sys.version_info >= q
    else:
        is_ok = sys.version_info > q
    if not is_ok:
        exit(f'calibre requires Python {minver}. Current Python version: {".".join(map(str, sys.version_info[:3]))}')


check_version_info()
