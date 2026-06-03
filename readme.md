
## Certs
Run this to generate certs, git-bash has openssl installed
```bash
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -sha256 -days 365 -nodes
```

## Testing
You need to have a gcc compiler to use the -race flag. This command runs the test suit 20 times and checks for race conditions 
```bash
go test -race -count=20
```
## Next steps / Comments
 - I currently dont have any topic clean up, every distinct topic creates one channel that lives forever,
does not matter at this scale but is a real issue.

- POST `/stats` creates a topic, but you cant read from that topic becasue its ambigous with static the GET/stats endpoint

- Add gracefull shutdown
  
- Add request body size limit

- FIFO Orderning is preserved by the channel sender wait queue.

