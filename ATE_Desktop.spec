# -*- mode: python ; coding: utf-8 -*-

# Все файлы остаются внутри dist\ATE_6PRO\ рядом с ATE_6PRO.exe.

a = Analysis(
    ['app/main.py'],
    pathex=['.'],
    binaries=[],
    datas=[
        ('data', 'data'),
        ('engine', 'engine'),
        ('okx', 'okx'),
        ('strategies', 'strategies'),
    ],
    hiddenimports=[
        'engine.controller',
    ],
    hookspath=[],
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='ATE_6PRO',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=False,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=False,
    name='ATE_6PRO',
)
