# Invoke-Parallel
Runs scriptblock in parallel with granular control over input, output, and processing.

## Example
```PowerShell
Invoke-Parallel -inputScript 'param($CN); Do-Thing -host $CN' -inputObject [array]$List -inputParam 'CN'
```

## ToDo
- Reserve threads for working operation (Suspend at timeout, run after rest of queue.)
- Emergency shutdown without shell closure. (Using Handles or Runspace distruction?)
- Allow threads to share data. (SynchronizedHashtable?)
