Dim shell
Dim patchesDir
Set objShell = CreateObject("Shell.Application")

patchesDir = Session.Property("INSTALLDIR") + "patch"
objShell.Explore patchesDir