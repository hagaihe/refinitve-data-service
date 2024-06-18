@echo off
REM Stop the existing container
for /f "tokens=* USEBACKQ" %%f in (`docker ps -q --filter "ancestor=refinitiv-data-service:latest"`) do (
    set existing_container=%%f
)

if not "%existing_container%"=="" (
    echo Stopping existing container %existing_container%...
    docker stop %existing_container%
    echo Removing existing container %existing_container%...
    docker rm %existing_container%
)

REM Load the new Docker image from the tarball
echo Loading new Docker image from refinitiv-data-service.tar...
docker load -i refinitiv-data-service.tar

REM Run the new container
REM echo Running new container from the loaded image...
REM docker run -d -p 8080:8080 -e REFINITIV_APP_KEY=your-app-key -e REFINITIV_USERNAME=your-username -e REFINITIV_PASSWORD=your-password refinitiv-data-service:latest

REM echo New container is running.
pause
