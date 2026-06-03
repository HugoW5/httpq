# Create a Publish Subscribe HTTP Server

httpQ is a simple broker that provides a number of HTTP "channels"

When a producer connects:
    it should connect by pushlishing a message to a topic
    if no consumers are waiting, it should block until a consumer connects on that channel

When a consumer connects:
    it should return the message published to a topic
    if no producers are waiting, it should block until a producer connects on that channel

No message persistence is required.  Ordering is considered:


Compile the following stats:
1. Failures to send (ie. Timeout)
2. Failures to receive (ie. Timeout)
3. Bytes received
4. Bytes published

Producer example query:

`curl -k https://localhost:24744/NhPvrxcJ5WfsYJ -d "hello 1"`
`curl -k https://localhost:24744/NhPvrxcJ5WfsYJ -d "hello 2"`

Consumer example query:

`curl -k https://localhost:24744/NhPvrxcJ5WfsYJ`

Output: `hello 1`

`curl -k https://localhost:24744/NhPvrxcJ5WfsYJ`

Output: `hello 2`

Stats example query:

`curk -k https://localhost:24744/stats`


1. Create a new project with git
2. Setup go module
3. Create binary with HTTPS server and self signed certificates
4. Complete tests to verify functionality


Time of commits is not considered, we acknowledge that you might not work on it all at once.
Time to complete is not considered.