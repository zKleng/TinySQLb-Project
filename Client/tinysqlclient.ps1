param (
    [Parameter(Mandatory = $true)]
    [string]$IP,
    [Parameter(Mandatory = $true)]
    [int]$Port
)

$ipEndPoint = [System.Net.IPEndPoint]::new([System.Net.IPAddress]::Parse($IP), $Port)
$CurrentDatabase = ""

function Send-Message {
    param (
        [Parameter(Mandatory=$true)]
        [pscustomobject]$message,
        [Parameter(Mandatory=$true)]
        [System.Net.Sockets.Socket]$client
    )

    $stream = New-Object System.Net.Sockets.NetworkStream($client)
    $writer = New-Object System.IO.StreamWriter($stream)
    try {
        $writer.WriteLine($message)
    }
    finally {
        $writer.Close()
        $stream.Close()
    }
}

function Receive-Message {
    param (
        [System.Net.Sockets.Socket]$client
    )
    $stream = New-Object System.Net.Sockets.NetworkStream($client)
    $reader = New-Object System.IO.StreamReader($stream)
    try {
        $line = $reader.ReadLine()
        if ($null -ne $line) {
            return $line
        } else {
            return ""
        }
    }
    finally {
        $reader.Close()
        $stream.Close()
    }
}

function Send-SQLCommand {
    param (
        [string]$command
    )
    
    # Si la sentencia no es SET DATABASE y tenemos una base de datos en contexto, prepende la base de datos actual
    if ($command -notmatch "^SET DATABASE" -and $CurrentDatabase -ne "") {
        $command = "USE DATABASE $CurrentDatabase; $command"
    }

    $client = New-Object System.Net.Sockets.Socket($ipEndPoint.AddressFamily, [System.Net.Sockets.SocketType]::Stream, [System.Net.Sockets.ProtocolType]::Tcp)
    $client.Connect($ipEndPoint)
    $requestObject = [PSCustomObject]@{
        RequestType = 0;
        RequestBody = $command
    }
    Write-Host -ForegroundColor Green "Sending command: $command"

    $jsonMessage = ConvertTo-Json -InputObject $requestObject -Compress
    Send-Message -client $client -message $jsonMessage
    $response = Receive-Message -client $client

    Write-Host -ForegroundColor Green "Response received: $response"
    
    $responseObject = ConvertFrom-Json -InputObject $response
    if ($responseObject.Status -eq 0) {
        if ($responseObject.ResponseBody -ne "") {
            Write-Output $responseObject.ResponseBody
        } else {
            Write-Host -ForegroundColor Yellow "Warning: Response body is empty."
        }

        # Si la sentencia fue SET DATABASE y fue exitosa, actualizar la base de datos en contexto
        if ($command -match "^SET DATABASE") {
            $CurrentDatabase = $command -replace "SET DATABASE", "" -replace ";", "" -replace "USE DATABASE", "" -replace "\s+", ""
            Write-Host -ForegroundColor Cyan "Contexto de base de datos establecido en: $CurrentDatabase"
        }

    } else {
        Write-Host -ForegroundColor Red "Error: $responseObject.ResponseBody"
    }

    $client.Shutdown([System.Net.Sockets.SocketShutdown]::Both)
    $client.Close()
}


