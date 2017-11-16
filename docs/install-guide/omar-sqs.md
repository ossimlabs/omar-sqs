# OMAR SQS

## Dockerfile
```
FROM omar-base
EXPOSE 8080
RUN mkdir /usr/share/omar
COPY omar-sqs-app-1.0.0-SNAPSHOT.jar /usr/share/omar
RUN chown -R 1001:0 /usr/share/omar
RUN chown 1001:0 /usr/share/omar
RUN chmod -R g+rw /usr/share/omar
RUN find $HOME -type d -exec chmod g+x {} +
USER 1001
WORKDIR /usr/share/omar
CMD java -server -Xms256m -Xmx1024m -Djava.awt.headless=true -XX:+CMSClassUnloadingEnabled -XX:+UseGCOverheadLimit -Djava.security.egd=file:/dev/./urandom -jar omar-sqs-app-1.0.0-SNAPSHOT.jar
```
Ref: [omar-base](../../../omar-base/docs/install-guide/omar-base/)

## JAR
[https://artifactory.ossim.io/artifactory/webapp/#/artifacts/browse/tree/General/omar-local/io/ossim/omar/apps/omar-sqs-app](https://artifactory.ossim.io/artifactory/webapp/#/artifacts/browse/tree/General/omar-local/io/ossim/omar/apps/omar-sqs-app)

## Configuration
Settings from the [Common Config Settings](../../../omar-common/docs/install-guide/omar-common/#common-config-settings) can be added to the base YAML definition:

* **queue** defines an Amazon SQS endpoint for access.
* **waitTimeSecond** This value can be between 1 and 20 and can not exceed 20 or you get errors and the service will not start proeprly.  This value is used by the AWS API to wait for a maximum time for a message to occur before returning.
* **maxNumberOfMessages** Value can only be between 1 and 10.  Any other value will give errors and the service will not start properly.  This defines the maximum number of messages to pop off the queue during a single read request to the service.
* **pollingIntervalSeconds** this can be any value and defines the number of second to *SLEEP* the background process between each call to the read request.  By default it will keep calling the read request until no messages are found.  After no messages are found the backgroun process will then *SLEEP* for **pollingIntervalSeconds**.
* **destination.type** This value can be either "post" or "stdout".   If the value is a post then it expects the **post** entry to be defined.  If the type is stdout then all message payload/message body are printed to standard out.
* **destination.post.urlEndPoint** Defines the url to post the message to.  The example here was taken from the ossim-vagrant implementation
