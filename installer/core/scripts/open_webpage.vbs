Option Explicit

Dim WscriptSchell
Set WscriptSchell = CreateObject("WScript.Shell")

WscriptSchell.Run Session.Property("WebPageURL"), 3
