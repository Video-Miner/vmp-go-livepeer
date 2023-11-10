## Define the service name in a variable
$ServiceName = 'videoMinerSvc'

## Read the service from Windows to return a service object
$ServiceInfo = Get-Service -Name $ServiceName

$Host.UI.RawUI.WindowTitle = "Video Miner - stopping"

## If the server is running (ne)
if ($ServiceInfo.Status -eq 'Running') {
	## Write to the console that the service is running
	Write-Host 'Service is running, stopping service...'
	## Start the service
	Start-Process -Verb RunAs -WindowStyle Hidden powershell.exe "Stop-Service -Name $ServiceName"
	while ($ServiceInfo.Status -eq 'Running') { 
		$ServiceInfo.Refresh()
		Start-Sleep -Seconds 0.25
	}
	Write-Host "service stopped."
} else { ## If the Status is anything but Running
	## Write to the console the service is already stopped
	Write-Host 'The service is already stopped.'
}
write-host "Press any key to close this window."
[void][System.Console]::ReadKey($true)