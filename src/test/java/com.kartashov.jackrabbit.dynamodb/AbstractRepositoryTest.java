package com.kartashov.jackrabbit.dynamodb;

import com.sun.org.apache.bcel.internal.Repository;
import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.core.RepositoryImpl;
import org.apache.jackrabbit.core.TransientRepository;
import org.apache.jackrabbit.core.config.ConfigurationException;
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
    public static void createRepository() throws URISyntaxException, RepositoryException {
        File configuration = new File(AbstractRepositoryTest.class.getResource("/repository.xml").toURI());
        RepositoryConfig repositoryConfig = RepositoryConfig.create(configuration.getParentFile());
        repository = new TransientRepository(repositoryConfig);
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
