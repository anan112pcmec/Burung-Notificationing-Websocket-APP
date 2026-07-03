Set-Location -Path 'c:\Burung_App\Project_Source\Backend-1'

$files = @(
    'C:\Burung_App\Project_Source\Backend-3\notification-app\initialize.go'
    # 'C:\Burung_App\Project_Source\Backend-3\notification-app\bwa_http_req\handling_req.go'
)

foreach ($file in $files) {
    if (Test-Path -Path $file) {
        $content = Get-Content -Raw -Path $file
        
        # Regex untuk mendeteksi '5 * time.Second' atau 'time.Second * 5' (fleksibel spasi)
        # \s* artinya spasi boleh ada atau tidak (misal: 5*time atau 5 * time)
        # $pattern = '(([1-9]|10)\s*\*\s*time\.Second|time\.Second\s*\*\s*([1-9]|10))'
        
        # Lakukan replace menggunakan regex pattern di atas
        $content = $content -replace 'prevision', 'environment'
        
        # Di Go, biasakan pakai UTF8 tanpa BOM agar compiler tidak protes
        [System.IO.File]::WriteAllText((Resolve-Path $file), $content, (New-Object System.Text.UTF8Encoding($false)))
        
        Write-Host "Updated $file" -ForegroundColor Green
    } else {
        Write-Warning "File tidak ditemukan: $file"
    }
}

Write-Host "Proses selesai!" -ForegroundColor Cyan