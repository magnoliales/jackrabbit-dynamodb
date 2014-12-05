DynamoDB Persistence Manager
============================

Add the following profile to your global maven configuration


To run execute `mvn test -Daws.accessKeyId=... -Daws.secretKey= -P magnolia development`
```xml
<profiles>
    <profile>
        <id>magnolia</id>
        <repositories>
            <repository>
                <id>magnolia.public.releases</id>
                <url>https://nexus.magnolia-cms.com/content/repositories/magnolia.public.releases</url>
            </repository>
            <repository>
                <id>thirdparty</id>
                <url>https://nexus.magnolia-cms.com/content/repositories/thirdparty</url>
            </repository>
            <repository>
                <id>thirdparty.customized</id>
                <url>https://nexus.magnolia-cms.com/content/repositories/thirdparty.customized</url>
            </repository>
            <repository>
                <id>vaadin-addons</id>
                <url>http://maven.vaadin.com/vaadin-addons</url>
            </repository>
            <repository>
                <id>maven.central</id>
                <url>http://repo1.maven.org/maven2</url>
            </repository>
        </repositories>
    </profile>
</profiles>
```

To Do
-----

- Move proper test suite from jackrabbit project