<#
  Synopsis:
    Runs scriptblock in parallel with granular control over input, output, and processing.
  Description:

  Contact:
    Caleb Worthington (Ktufuathama) https://github.com/Ktufuathama
  Version:
    v.0.A - 06OCT17 - PreRelease Testing
    v.1.0 - 12OCT17 - InitialRelease
    v.2.0 - 13OCT17 - Rework (Error handling, Runtime, Logging, Progressbar)
    v.2.1 - 01OCT17 - Fix (Time_Catch removed)
    v.2.2 - 15NOV17 - Added (Start-Time, fixed runtimekill and limit)
    v.3.0 - 27NOV18 - Rework (Threshold, new logging, Optimization)
    v.3.1 - 14DEC18 - Added decription and some code cleanup.
    v.3.2 - 18MAR19 - Grammer fixes.
    v.3.3 - 11JUN19 - More Grammer and formatting fixes.
    v.4.0 - 21NOV19 - Moved all logic to class with wrapper function.
    v.4.1 - 03DEC19 - Add RuntimeLimit (broken in implimentation), CtrlC will cancel remaining executions.
  Parameters:
    InputScript - Script to execute {scriptblock}.
    InputObject - Object to run in parallel.
    InputParam - ParameterName to assign the current InputObject. 'ComputerName'
    Parameters - Hashtable of additional parameters. @{Class = 'win32_product'}
    Throttle - Max number of simultaneous running executions.
    Threshold - Max number of staged/loaded executions at anytime.
    RuntimeLimit - One limit is hit, all remain executions are cancelled.
    Quiet - Suppress progressbar.
    Raw - Execution output is sent to pipeline instead of stored in [Parallel] instance.
  Input:
    No Pipeline support, parameters only.
  Output:
    If '-raw' is passed, will return scriptblock output.
    If '-raw' is not passed, will return [Parallel] class object.
  Examples:
    Invoke-Parallel -inputScript 'param($CN); Do-Thing -host $CN' -inputObject [array]$List -inputParam 'CN'
  ToDo:
    Reserve Threads for working operation (Suspend at timeout, run after rest of queue.)
    Emergency shutdown without shell closure. (Using Handles or Runspace distruction?)
    Allow threads to share data. (SynchronizedHashtable?)
#>
function Invoke-Parallel {
  [cmdletbinding()]
  param(
    [string]$InputScript,
    [object[]]$InputObject,
    [string]$InputParam,
    [hashtable]$Parameters = @{},
    [int]$Throttle = [int]$env:NUMBER_OF_PROCESSORS,
    [int]$Threshold = ([int]$env:NUMBER_OF_PROCESSORS * 2),
    [double]$RuntimeLimit = 10,
    [switch]$Quiet,
    [switch]$Raw
  )
  $Parallel = [parallel]::new()
  $Parallel.PassThru  = $Raw
  $Parallel.Throttle  = $Throttle
  $Parallel.Threshold = $Threshold
  $Parallel.RuntimeLimit = $RuntimeLimit
  $Parallel.CycleTime = $CycleTime
  $Parallel.Progress  = !$Quiet
  $Parallel.invokeParallel($InputObject, $InputParam, $Parameters, $InputScript)
  if ($Raw) {
    $Parallel.dispose()
    return
  }
  return $Parallel
}

class Parallel
{
  
  [system.collections.hashtable]$Results = [system.collections.hashtable]::new()
  [system.collections.arraylist]$Errors  = [system.collections.arraylist]::new()

  <#ToDo - Allow threads to share data.
    [system.collections.hashtable]$global:Sync = [system.collections.hashtable]::synchronized(
      [system.collections.hashtable]::new())
  #>

  [int]$Throttle  = 10
  [int]$Threshold = 100
  [int]$CycleTime = 200
  [double]$RuntimeLimit = 10
  [bool]$Verbose  = $false
  [bool]$Progress = $true
  [bool]$Debug    = $false
  [bool]$PassThru = $false

  hidden [datetime]$InitTime
  hidden [int]$ExecutionId
  hidden [int]$InputObjectCount
  hidden [int]$StagedCount
  hidden [int]$CompletedCount
  hidden [system.management.automation.runspaces.runspacepool]$Pool
  hidden [system.collections.arraylist]$Executions = [system.collections.arraylist]::new()
  hidden [system.collections.queue]$Queue = [system.collections.queue]::new()

  InvokeParallel(
    [object[]]$InputObject,
    [string]$InputParam,
    [hashtable]$Parameters,
    [string]$InputScript)
  {
    try {
      $this.InitTime = [datetime]::Now
      $this.writeVerbose("Operation Started!")
      [console]::TreatControlCAsInput = $true
      $this.Pool = [system.management.automation.runspaces.runspacefactory]::createRunspacePool(1, $this.Throttle)
      $this.Pool.ApartmentState = 'MTA'
      $this.Pool.open()
      $this.Results.clear()
      $this.InputObjectCount = $InputObject.Count
      $this.StagedCount = 0
      $this.CompletedCount = 0
      $this.ExecutionId = 0
      $this.writeDebug("Prestaged")
      for ($I = 0; $I -lt $InputObject.Count; $I++) {
        $this.writeVerbose("[$($I + 1)] Execution Started")
        $this.writeProgress(0)
        $this.invokeExecution($InputScript, $InputObject[$I], $InputParam, $Parameters)
        $this.StagedCount++
        $this.wait($this.Threshold, 1)
      }
      $this.writeDebug("Poststaged")
      $this.wait(0, 0)
    }
    catch {
      $this.Errors.add($_)
    }
    finally {
      $this.writeProgress(1)
      $this.writeDebug("Executions Finshed.")
      $this.Pool.close()
      $this.Pool.dispose()
      [console]::TreatControlCAsInput = $false
      $this.writeVerbose("Operation Finished!")
    }
  }

  hidden InvokeExecution(
    [string]$InputScript,
    [object]$InputObject,
    [string]$InputParam,
    [hashtable]$Parameters)
  {
    try {
      $Execution = [powershell]::create()
      $Execution.addScript($InputScript, $true)
      $Execution.addParameter($InputParam, $InputObject)
      foreach ($Key in $Parameters.Keys) {
        $Execution.addParameter($Key, $Parameters.$Key)
      }
      $Execution.RunspacePool = $this.Pool
      $Return = [system.management.automation.psdatacollection[psobject]]::new()
      $this.ExecutionId++
      $this.Executions.add([pscustomobject]@{
        Id        = $this.ExecutionId
        StartTime = [datetime]::Now
        Handle    = $Execution.beginInvoke($Return, $Return)
        Execution = $Execution
        Object    = $InputObject.toString()
        Return    = $Return
      })
    }
    catch {
      $this.Errors.add($_)
    }
  }

  Wait([int]$Threshold, [int]$Offset)
  {
    try {
      while ($this.Executions.Count -gt ($Threshold - $Offset)) {
        $this.writeProgress(0)
        $Key = $null
        if ($global:Host.UI.RawUI.KeyAvailable -and ($Key = $global:Host.UI.RawUI.readKey("AllowCtrlC,NoEcho,IncludeKeyUp"))) {
          if ([int]$Key.Character -eq 3) {
            for ($I = 0; $I -lt $this.Executions.Count; $I++) {
              $this.Executions[$I].Return = "Cancelled:UserAction"
              $this.Executions[$I].Execution = $null
              $this.returnResults($I)
              $this.Queue.enqueue($this.Executions[$I])
              $this.CompletedCount++
            }
          }
          $global:Host.UI.RawUI.flushInputBuffer()
        }
        for ($I = 0; $I -lt $this.Executions.Count; $I++) {
          if ($this.Executions[$I].StartTime.addMinutes($this.RuntimeLimit) -lt $([datetime]::Now)) {
            $this.Executions[$I].Return = "Cancelled:ExceededRuntime"
            $this.Executions[$I].Execution = $null
            $this.returnResults($I)
            $this.Queue.enqueue($this.Executions[$I])
            $this.CompletedCount++
          }
          if ($this.Executions[$I].Handle.IsCompleted -eq $true) {
            $this.writeVerbose("[$($this.Executions[$I].Id)] Execution Completed")
            $this.Executions[$I].Execution.endInvoke($this.Executions[$I].Handle)
            $this.Executions[$I].Execution.dispose()
            $this.returnResults($I)
            $this.Queue.enqueue($this.Executions[$I])
            $this.CompletedCount++
          }
        }
        while ($this.Queue.Count -gt 0) {
          $this.Executions.remove($this.Queue.dequeue())
        }
        Start-Sleep -milliseconds $this.CycleTime
      }
    }
    catch {
      $this.Errors.add($_)
    }
  }

  ReturnResults([int]$I)
  {
    if ($this.PassThru) {
      Out-Host -inputObject @{
        $this.Executions[$I].Object = $this.Executions[$I].Return
      }
    }
    else {
      if ($this.Debug) {
        $this.Results.add($this.Executions[$I].Object, $this.Executions[$I])
      }
      else {
        $this.Results.add($this.Executions[$I].Object, $this.Executions[$I].Return)
      }
    }
  }

  WriteVerbose([string]$Message)
  {
    $Splat = @{
      #Message = "$(([datetime]::Now - $this.InitTime).toString().subString(0, 11)) $($Message)"
      Message = "$($Message)"
      Verbose = $this.Verbose
    }
    Write-Verbose @Splat
  }

  WriteDebug([string]$Message)
  {
    $Splat = @{
      #Message = "$(([datetime]::Now - $this.InitTime).toString().subString(0, 11)) $($Message)"
      Message = "$($Message)"
      Debug = $this.Debug
    }
    Write-Debug @Splat
  }

  WriteProgress([int]$Code)
  {
    if ($this.Progress) {
      switch ($Code) {
        0 {
          $1 = $([datetime]::Now - $this.InitTime).toString().split('.')[0]
          $2 = "$($this.CompletedCount)/$($this.StagedCount)/$($this.InputObjectCount)"
          $3 = (($this.Throttle - $this.Pool.getAvailableRunspaces()) / $this.Throttle * 100)
          $4 = "/$($this.Throttle)/$($this.Threshold)/$($this.CycleTime)ms"
          Write-Progress -activity "$($1) - $($2) [$($3)%$($4)]"
        }
        1 {
          $1 = $([datetime]::Now - $this.InitTime).toString().split('.')[0]
          Write-Progress -activity "$($1) - Disposing Executions..."
        }
      }
    }
  }

  [bool] Dispose() {
    $this.Errors.clear()
    $this.Results.clear()
    $this.Pool = $null
    return $true
  }
}
