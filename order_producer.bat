
@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM ======= Configuration =======
set "BOOTSTRAP=localhost:9092"
set "TOPIC=mon_topic_replique"

REM Adapter selon ton installation Kafka
set "KAFKA_HOME=C:\kafka"
set "PRODUCER=%KAFKA_HOME%\bin\windows\kafka-console-producer.bat"

REM 30 commandes / minute
set "SLEEP_SECONDS=2"

REM Point de depart des IDs
set /a ID=1452

REM ======= Verification =======
if not exist "%PRODUCER%" (
    echo ERREUR : Kafka Producer introuvable :
    echo %PRODUCER%
    pause
    exit /b 1
)

echo.
echo ===== Kafka Orders Producer Simulator =====
echo Bootstrap: %BOOTSTRAP%
echo Topic:     %TOPIC%
echo Rate:      30 msg/min
echo Press CTRL+C to stop.
echo.

REM ======= Boucle infinie =======
:LOOP

REM Produit aleatoire entre 0 et 5
set /a R=!RANDOM! %% 6

if !R! EQU 0 (
    set "PRODUCT=laptop"
    set /a PRICE=500
)
if !R! EQU 1 (
    set "PRODUCT=mouse"
    set /a PRICE=25
)
if !R! EQU 2 (
    set "PRODUCT=keyboard"
    set /a PRICE=70
)
if !R! EQU 3 (
    set "PRODUCT=screen"
    set /a PRICE=180
)
if !R! EQU 4 (
    set "PRODUCT=headset"
    set /a PRICE=60
)
if !R! EQU 5 (
    set "PRODUCT=ssd"
    set /a PRICE=120
)

REM Quantite aleatoire entre 1 et 4
set /a QTY=(!RANDOM! %% 4) + 1

REM Prix total
set /a TOTAL=PRICE * QTY

REM Timestamp ISO via PowerShell
for /f "delims=" %%T in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-ddTHH:mm:ss"') do (
    set "TS=%%T"
)

REM Construction du JSON
set "JSON={ "id_order": !ID!, "product_name": "!PRODUCT!", "nombre": !QTY!, "total_price": !TOTAL!, "ts": "!TS!" }"

echo Sending: !JSON!

REM Envoi du message vers Kafka
echo(!JSON! | call "%PRODUCER%" --bootstrap-server "%BOOTSTRAP%" --topic "%TOPIC%" >nul

if errorlevel 1 (
    echo ERREUR : Echec du producteur Kafka.
)

REM Incrementer ID
set /a ID+=1

REM Attendre 2 secondes
timeout /t %SLEEP_SECONDS% /nobreak >nul

goto LOOP
