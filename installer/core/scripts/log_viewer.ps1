param ($directory='.')
Set-Location $directory

$Host.UI.RawUI.WindowTitle = "Video Miner Logs - Closing will not stop Video Miner"

Write-Host "IMPORTANT!! The Video Miner application will continue to run even when this window is closed." -ForegroundColor red
Start-Sleep -Seconds 0.25
Write-Host "`r`n" -NoNewline
Write-Host "***************************************`r`n" -NoNewline
Write-Host "****** OPENING VIDEO MINER LOGS  ******`r`n" -NoNewline
Write-Host "****** LIVE VIEWING RECENT LINES ******`r`n" -NoNewline
Write-Host "***************************************`r`n" -NoNewline


Write-Host "Waiting for log file to appear...." -ForegroundColor green

$counter = 0
while (!(Test-Path "stderr.log") -and ($counter -lt 15)) { 
    Start-Sleep -Seconds 0.25
}

if(!(Test-Path "stderr.log")) {
    Write-Host "Could not find the file after waiting.  Check that service is running and try again."
    Exit
}
Get-Content stderr.log -Tail 100 -Wait
