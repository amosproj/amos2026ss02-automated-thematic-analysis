#Requires -Version 5.1
# Convenience wrapper - equivalent to: .\setup.ps1 -Down
# Pass -Yes to also skip the confirmation prompt.
# Pass -DownVolumes to remove data volumes as well.
& "$PSScriptRoot\setup.ps1" -Down @args
