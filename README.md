DynamoDB Persistence Manager
============================

To run execute `mvn test -Daws.accessKeyId=... -Daws.secretKey= -P magnolia,development`

To Do
-----

- Move proper test suite from jackrabbit project
- Fix date serialization
- Add retries to fetching and writing of the content
- Scan doesn't work the right way (fix)