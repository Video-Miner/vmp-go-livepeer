## Define the service name in a variable
$ServiceName = 'videoMinerSvc'

## Read the service from Windows to return a service object
$ServiceInfo = Get-Service -Name $ServiceName

$Host.UI.RawUI.WindowTitle = "Video Miner - starting"

## If the server is not running (ne)
if ($ServiceInfo.Status -ne 'Running') {
	## Write to the console that the service is not running
	Write-Host 'Service is not started, starting service...'
	## Start the service
	Start-Process -Verb RunAs -WindowStyle Hidden powershell.exe "Start-Service -Name $ServiceName"
	
	while ($ServiceInfo.Status -ne 'Running') {
		$ServiceInfo.Refresh()
		Start-Sleep -Seconds 0.25
	}
	Write-Host "service started."
} else { ## If the Status is anything but Running
	## Write to the console the service is already running
	Write-Host 'The service is already running.'
}
write-host "Press any key to close this window."
[void][System.Console]::ReadKey($true)