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
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.lang.StringUtils;

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
                                if(e.isEmpty()) {
                                    outPrintStream.printf("%n"); // EOL
                                } else {
                                    //outPrintStream.printf("%s%n", e);
                                    final String[] tuple = keyValueSplitter(e);
                                    outPrintStream.printf("%s='%s'  ", tuple[0], resolve(db, tuple[0], tuple[1]));
                                }
                            } catch(final Exception exception) {
                                log.error("unexpected error on " + e, exception);
                            }
                        });
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
        case "IDENTITY":
            {
                final User result = findUseByID(connection, value);
                return (result != null) ? result.toString() : value;
            }
            
        default: return value;
        }
    }






    /////////////////////////////////////////////////////////////////////////
    //                            Database Methods                         //
    /////////////////////////////////////////////////////////////////////////

    
    public static User findUseByID(final Connection connection, final String ID) throws SQLException {

        //System.out.println("ID IS " + ID);
        val run = new QueryRunner();
        return run.query(connection, "SELECT p.id, p.loginname, p.firstname, p.lastname, p.email  FROM accounttypeentity p WHERE p.id = ?", new BeanHandler<User>(User.class), ID);
    }

    
    public static Set<User> findAllUsers(final Connection connection, final Set<String> ids, final int iLimit, final int iOffset) throws SQLException {
        if(ids == null || ids.isEmpty()) {
            return new HashSet<>();
        }
        
        val run = new QueryRunner();
        val quotedList = ids.stream().map(id -> "'" + id + "'").collect(Collectors.toList());
        val SQL = String.format("SELECT p.id, p.loginname, p.firstname, p.lastname, p.email  FROM accounttypeentity p WHERE p.id IN (%s) ORDER BY UPPER(p.loginname) ASC LIMIT ? OFFSET ?", StringUtils.join(quotedList, ","));
        
        return new HashSet<>(run.query(connection, SQL , new BeanListHandler<User>(User.class), iLimit, iOffset));
    }
    
}
