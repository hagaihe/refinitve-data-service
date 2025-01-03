Building and Porting the Docker Image:

To build the Docker image, use the docker build command. Here is the general syntax: docker build -t refinitiv-data-service: latest .

Once the image is built, you can save it to a file using the docker save command. T
his creates a tarball of the image which can be transferred to another machine.

docker save -o refinitiv-data-service.tar refinitiv-data-service: latest

Transfer the tar file to the other machine using a method of your choice (e.g., SCP, FTP, etc.).

On the other machine, load the image from the tarball using the docker load command: docker load -i refinitiv-data-service.tar

After loading the image, you can run a container from it using the docker run command: docker run -d -p 8080:8080 \
-e REFINITIV_APP_KEY=<'your-app-key'> \
-e REFINITIV_USERNAME=<'your-username'> \
-e REFINITIV_PASSWORD=<'your-password'> \
refinitiv-data-service: latest

in windows server run the docker without quotes, as followed -

docker run -d -p 8080: 8080 \
-e REFINITIV_APP_KEY=<your-app-key> \
-e REFINITIV_USERNAME=<your-username> \
-e REFINITIV_PASSWORD=<your-password> \
refinitiv-data-service: latest
