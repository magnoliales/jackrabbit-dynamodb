DynamoDB Persistence Manager
============================

To run execute `mvn test -Daws.accessKeyId=... -Daws.secretKey=`

To Do
-----

- Move proper test suite from jackrabbit project
- Fix date serialization
- Add retries to fetching and writing of the content
- Add mapper for the rest of the property types
- Add check consistency flag
- Check that the table has the proper format