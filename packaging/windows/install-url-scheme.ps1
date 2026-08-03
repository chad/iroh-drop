# install-url-scheme.ps1 — register the iroh-drop:// URL scheme for the
# current user (no admin needed), so links like
#
#   iroh-drop://receive/drop1...
#
# clicked in chat, mail, or a browser open in Drop.exe and join the drop.
#
# Run once after unzipping; re-run if you move the folder. To remove:
#   Remove-Item -Recurse HKCU:\Software\Classes\iroh-drop

$exe = Join-Path $PSScriptRoot "Drop.exe"
if (-not (Test-Path $exe)) {
    Write-Error "Drop.exe not found next to this script — run it from inside the iroh-drop folder."
    exit 1
}

$base = "HKCU:\Software\Classes\iroh-drop"
New-Item -Path $base -Force | Out-Null
Set-ItemProperty -Path $base -Name "(Default)" -Value "URL:iroh-drop Protocol"
Set-ItemProperty -Path $base -Name "URL Protocol" -Value ""

New-Item -Path "$base\DefaultIcon" -Force | Out-Null
Set-ItemProperty -Path "$base\DefaultIcon" -Name "(Default)" -Value "`"$exe`",0"

New-Item -Path "$base\shell\open\command" -Force | Out-Null
Set-ItemProperty -Path "$base\shell\open\command" -Name "(Default)" -Value "`"$exe`" `"%1`""

Write-Host "Registered iroh-drop:// -> $exe"
Write-Host ""
Write-Host "Verify it (with Drop running or not):"
Write-Host '  Start-Process "iroh-drop://receive/<paste a real drop1 link here>"'
Write-Host "Drop should open (or a second window appear) already joining the drop."
