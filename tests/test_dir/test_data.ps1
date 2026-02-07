$valves = @()
$io = @()

for ($i = 1; $i -le 1000; $i++) {

    $valveName = "V{0:D3}" -f $i
    $openId = "IO_OPEN_$i"
    $closedId = "IO_CLOSED_$i"

    # Valve entry
    $valves += @{
        name = $valveName
        open_feedback = $openId
        closed_feedback = $closedId
    }

    # IO entries (NOTE: numbers are NOT quoted)
    $io += @{
        address = 0
        db = $i
        id = $openId
        rack = 0
    }

    $io += @{
        address = 1
        db = $i
        id = $closedId
        rack = 0
    }
}

$valvesJson = @{ valves = $valves }
$ioJson = @{ io = $io }

$utf8NoBom = New-Object System.Text.UTF8Encoding($false)  # $false = no BOM

[System.IO.File]::WriteAllText("data\valves.json", ($valvesJson | ConvertTo-Json -Depth 5), $utf8NoBom)
[System.IO.File]::WriteAllText("data\io.json",     ($ioJson     | ConvertTo-Json -Depth 5), $utf8NoBom)
