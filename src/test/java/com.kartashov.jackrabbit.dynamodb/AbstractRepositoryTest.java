package com.kartashov.jackrabbit.dynamodb;

import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.core.RepositoryImpl;
import org.apache.jackrabbit.core.TransientRepository;
import org.apache.jackrabbit.core.config.RepositoryConfig;
import org.junit.*;

import javax.jcr.Credentials;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import java.io.File;
import java.net.URISyntaxException;

public abstract class AbstractRepositoryTest {

    protected static JackrabbitRepository repository;
    protected static Session session;

    @BeforeClass
    public static void createRepository() throws URISyntaxException {
        File configuration = new File(AbstractRepositoryTest.class.getResource("/repository.xml").toURI());
        File directory = new File(System.getProperty("java.io.tmpdir"));
        repository = new TransientRepository(configuration, directory);
    }

    @AfterClass
    public static void removeRepository() {
        repository.shutdown();
    }

    @Before
    public void login() throws RepositoryException {
        if (session != null) {
            logout();
        }
        Credentials credentials = new SimpleCredentials("admin", "admin".toCharArray());
        session = repository.login(credentials);
    }

    @After
    public void logout() {
        if (session != null) {
            try {
                session.logout();
            } finally {
                session = null;
            }
        }
    }
}
