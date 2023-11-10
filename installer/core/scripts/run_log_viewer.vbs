Option Explicit

Dim WscriptSchell
Dim InstallDir

Set WscriptSchell = CreateObject("WScript.Shell")
InstallDir = Session.Property("INSTALLDIR")

' need to remove trailing slashes from path like "C:\Program Files\Video Miner Pool\"
if Right(InstallDir,1) = "\" then
    InstallDir=Left(InstallDir,Len(InstallDir)-1)
end if

WscriptSchell.Run "powershell  -ExecutionPolicy Bypass -NoExit -NonInteractive -file """ + InstallDir + "\log_viewer.ps1"" -directory """ + InstallDir + """", 3
