/*  Copyright (c) 2015
 *  by Bjönd Health, Inc., Boston, MA
 *
 *  This software is furnished under a license and may be used only in
 *  accordance with the terms of such license.  This software may not be
 *  provided or otherwise made available to any other party.  No title to
 *  nor ownership of the software is hereby transferred.
 *
 *  This software is the intellectual property of Bjönd Health, Inc.,
 *  and is protected by the copyright laws of the United States of America.
 *  All rights reserved internationally.
 *
 */

package com.bjond;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.lang.StringUtils;

import com.bjond.entities.StringStub;
import com.bjond.entities.User;

import lombok.val;
import lombok.extern.slf4j.Slf4j;


/** <p> Main entry point for the application </p>

 *
 * <a href="mailto:Stephen.Agneta@bjondinc.com">Steve 'Crash' Agneta</a>
 *
 */
@Slf4j
public class Main  {
    private static final String OPENSHIFT_POSTGRESQL_DB_HOST     = System.getenv("OPENSHIFT_POSTGRESQL_DB_HOST");
    private static final String OPENSHIFT_POSTGRESQL_DB_PORT     = System.getenv("OPENSHIFT_POSTGRESQL_DB_PORT");
    private static final String OPENSHIFT_APP_NAME               = System.getenv("OPENSHIFT_APP_NAME");
    private static final String OPENSHIFT_POSTGRESQL_DB_USERNAME = System.getenv("OPENSHIFT_POSTGRESQL_DB_USERNAME");
    private static final String OPENSHIFT_POSTGRESQL_DB_PASSWORD = System.getenv("OPENSHIFT_POSTGRESQL_DB_PASSWORD");

    static String POSTGRESQL_URL; 



    // Caches
    private static Map<String, User> userMap = new HashMap<>();     // ID, User
    private static Map<String, String> tenantMap = new HashMap<>(); // ID, name
    
    
    /**
	 *  Main entry point. Takes no parameters. Just accepts stdin and outputs to
     * stdout. Old school...
	 * 
	 * @param args
	 * 
	 * @throws IOException
	 * @throws SQLException
	 */
    public static void main(String[] args) throws IOException, SQLException {
        process(System.in, System.out);
    }


    private static void process(final InputStream in , final OutputStream out) throws IOException, SQLException {
        log.info("Execution begins...");
        
        // Generate the POSTGRESQL URL form system envirionment variables.
        POSTGRESQL_URL = String.format("jdbc:postgresql://%s:%s/%s", OPENSHIFT_POSTGRESQL_DB_HOST, OPENSHIFT_POSTGRESQL_DB_PORT, OPENSHIFT_APP_NAME); 

        try(final Connection db = DriverManager.getConnection(POSTGRESQL_URL, OPENSHIFT_POSTGRESQL_DB_USERNAME, OPENSHIFT_POSTGRESQL_DB_PASSWORD);) {
        
         
            final PrintStream outPrintStream = new PrintStream(out, true, "UTF-8");
            final Reader inReader = new InputStreamReader(in, "UTF-8");
            final Iterable<CSVRecord> records = CSVFormat.DEFAULT.withQuote('\'').parse(inReader);

            //outPrintStream.printf("%s%n%n", POSTGRESQL_URL);
            log.info("PostgreSQL DB connectiion valid: {}", db.isValid(1000));
        
            records.forEach(record -> {
                    record.iterator().forEachRemaining(e -> {
                            try {
                                if(!e.isEmpty()) {
                                    final String[] tuple = keyValueSplitter(e);
                                    outPrintStream.printf("%s='%s',", tuple[0], resolve(db, tuple[0], tuple[1]));
                                }
                            } catch(final Exception exception) {
                                log.error("unexpected error on " + e, exception);
                            }
                        });

                    outPrintStream.printf("%n"); // EOL
                });
        }

        log.info("Execution ends...");
    }

    

    private static String[] keyValueSplitter(final String element) {
        // key=value split on equals
        String[] s = element.split("=");

        // Remove quotes
        s[1] = StringUtils.remove(s[1], '\'');
        
        return s;
    }
    
    
    private static String resolve(final Connection connection, final String key, final String value) throws SQLException {
        switch(key) {
        case "CREATERECORDLOGIN":
        case "READ/VIEWRECORDLOGIN":
        case "UPDATERECORDLOGIN":
        case "DELETERECORDLOGIN":
        case "IDENTITYCREATED":
        case "IDENTITYDELETED":
        case "USER":
        case "LOGIN":
        case "LOGOUT":
        case "IDENTITY":
                final User result = findUserByID(connection, value);
                return (result != null) ? result.toString().replace(',', '|') : value;

        case "TENANT":
        case "GROUP":
        case "GROUPCREATED":
        case "GROUPDELETED":
        case "GROUPROLEADDED":
        case "GROUPROLEREVOKED":
        case "DEFAULTTENANTDIVISIONCHANGEDGROUPID":
            return findGroupNameByID( connection, value);
                    
        default: return value;
        }
    }






    /////////////////////////////////////////////////////////////////////////
    //                            Database Methods                         //
    /////////////////////////////////////////////////////////////////////////

    

    public static String findGroupNameByID(final Connection connection, final String ID) throws SQLException {
        String name  = tenantMap.get(ID);
        if(name != null ) { return name;}

        val run = new QueryRunner();
        val stub = run.query(connection, "SELECT p.name FROM grouptypeentity p WHERE p.id = ?", new BeanHandler<StringStub>(StringStub.class), ID);

        if(stub != null )
            {
                tenantMap.put(ID, stub.getName());
                return stub.getName();
            }
        else {
            return ID;
        }
    }
        
    public static User findUserByID(final Connection connection, final String ID) throws SQLException {

        // Cached?
        User user = userMap.get(ID);
        if(user != null ) { return user;}

        val run = new QueryRunner();
        user =  run.query(connection, "SELECT p.id, p.loginname, p.firstname, p.lastname, p.email  FROM accounttypeentity p WHERE p.id = ?", new BeanHandler<User>(User.class), ID);

        // Cache it
        if(user != null ) { userMap.put(ID, user);}

        return user;
    }

    
}
