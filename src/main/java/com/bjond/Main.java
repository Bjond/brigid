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

import com.bjond.entities.PersonStub;
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
    static String lastRecordID;


    // Caches
    private static Map<String, User> userMap = new HashMap<>();     // ID, User
    private static Map<String, String> nameIDMap = new HashMap<>(); // ID, name
    private static Map<String, PersonStub> personMap = new HashMap<>();     // ID, User
    
    
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


    /**
	 *  Given an input stream _in_ to an audit log, the unobfuscated log will be stream to _out_.
	 * 
	 * @param in
	 * @param out
	 * 
	 * @throws IOException
	 * @throws SQLException
	 */
    public static void process(final InputStream in , final OutputStream out) throws IOException, SQLException {
        log.info("Execution begins...");
        
        // Generate the POSTGRESQL URL form system envirionment variables.
        POSTGRESQL_URL = String.format("jdbc:postgresql://%s:%s/%s", OPENSHIFT_POSTGRESQL_DB_HOST, OPENSHIFT_POSTGRESQL_DB_PORT, OPENSHIFT_APP_NAME); 

        try(final Connection db = DriverManager.getConnection(POSTGRESQL_URL, OPENSHIFT_POSTGRESQL_DB_USERNAME, OPENSHIFT_POSTGRESQL_DB_PASSWORD);) {
        
            final PrintStream outPrintStream = new PrintStream(out, true, "UTF-8");
            final Reader inReader = new InputStreamReader(in, "UTF-8");
            final Iterable<CSVRecord> records = CSVFormat.DEFAULT.withQuote('\'').parse(inReader);

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

    

    /**
	 *  Simply splits a key=value pair and removes single quotes.
	 * 
	 * @param element
	 * @return
	 */
    private static String[] keyValueSplitter(final String element) {
        // key=value split on equals
        final String[] s = element.split("=");

        // Remove quotes
        s[1] = StringUtils.remove(s[1], '\'');
        
        return s;
    }
    
    
    /**
	 *  Performs the resolution process of accepting na opaque key/value pair
     *  and returns an unobfuscated value. If no value can be resolved then value
     *  is simply returned unaltered.
	 * 
	 * @param connection
	 * @param key
	 * @param value
	 * @return
	 * 
	 * @throws SQLException
	 */
    private static String resolve(final Connection connection, final String key, final String value) throws SQLException {
        switch(key) {

        case "ASSIGNED_TO":
        case "ASSIGNOR":
        case "ALREADYLOGGEDIN":
        case "LOCKEDACCOUNT":
        case "AUTHORIZATIONFAILURE":
        case "LIMITEDIDENTITYCREATED":
        case "ACCOUNTLOCKEDFORLOGIN":
        case "ACCOUNTUNLOCKEDFORLOGIN":
        case "PASSWORDVALIDATIONFAILUREFORLOGIN":
        case "CLEAREDPASSWORDATTEMPTCOUNTERFORLOGIN":
        case "IDENTITYCREATED":
        case "IDENTITYDELETED":
        case "USER":
        case "LOGIN":
        case "LOGOUT":
        case "IDENTITY":
            final User result = findUserByID(connection, value);
            return (result != null) ? result.toString().replace(',', '|') : value;

        case "TASKSTATETRANSITIONTENANT":            
        case "TENANT":
        case "GROUP":
        case "GROUPCREATED":
        case "GROUPDELETED":
        case "GROUPROLEADDED":
        case "GROUPROLEREVOKED":
        case "DEFAULTTENANTDIVISIONCHANGEDGROUPID":
            return findGroupNameByID( connection, value);
                    
        case "ROLECREATED":
        case "ROLEDELETED":
        case "ROLEGRANTED":
        case "ROLEREVOKED":
        case "ROLEUPDATED":
            return findRoleNameByID(connection, value);

        case "CREATERECORDLOGIN":
        case "READ/VIEWRECORDLOGIN":
        case "UPDATERECORDLOGIN":
        case "DELETERECORDLOGIN":
            lastRecordID = value;
            return value;

        case "CLASS":
            return resolveClass(connection, value, lastRecordID);



            
        default:
            return value;
        }
    }






    /**
	 * Given a class (a full package name plus classname), resolve the ID to a name or more.
	 * 
	 * @param connection
	 * @param clazz
	 * @param ID
	 * @return
	 * 
	 * @throws SQLException
	 */
    public static String resolveClass(final Connection connection, final String clazz, final String ID ) throws SQLException {
        switch(clazz) {
        case "com.bjond.persistence.assessment.Assessment": return "Assessment: " + findAssessmentNameByID(connection,  ID);
        case "com.bjond.persistence.task.BjondTask": return "BjondTask: " + findBjondTaskNameByID(connection,  ID);
        case "com.bjond.persistence.permissions.UserDefinedRole": return "UserDefinedRole: " + findUserDefinedRoleTaskNameByID(connection,  ID);
        case "com.bjond.persistence.person.PersonPerson": return "Person: " + findPersonPersonByID(connection,  ID);
        case "com.bjond.persistence.rule.RuleDefinition": return "RuleDefinition: " + findRuleDefinitionNameByID(connection,  ID);
        case "com.bjond.persistence.tags.TagsFullText": return "Tag: " + findTagNameByID(connection,  ID);

        default:
            // Handles all questions identically.
            if(clazz.startsWith("com.bjond.persistence.assessment.Question")) {
                return clazz + ": " + findQuestionNameByID(connection,  ID);
            }
            return ID;
        }
        

    }
    

    /////////////////////////////////////////////////////////////////////////
    //                            Database Methods                         //
    /////////////////////////////////////////////////////////////////////////

    
    public static String findTagNameByID(final Connection connection, final String ID) throws SQLException {
        String name  = nameIDMap.get(ID);
        if(name != null ) { return name;}

        val run = new QueryRunner();
        val stub = run.query(connection, "SELECT p.name FROM tags_fulltext p WHERE p.id = ?", new BeanHandler<StringStub>(StringStub.class), ID);

        if(stub != null ) {
            nameIDMap.put(ID, stub.getName());
            return stub.getName();
            }
        else {
            return ID;
        }
    }

    
    public static String findQuestionNameByID(final Connection connection, final String ID) throws SQLException {
        String name  = nameIDMap.get(ID);
        if(name != null ) { return name;}

        val run = new QueryRunner();
        val stub = run.query(connection, "SELECT p.name FROM assessment_questions p WHERE p.id = ?", new BeanHandler<StringStub>(StringStub.class), ID);

        if(stub != null ) {
            nameIDMap.put(ID, stub.getName());
            return stub.getName();
            }
        else {
            return ID;
        }
    }

    public static String findRuleDefinitionNameByID(final Connection connection, final String ID) throws SQLException {
        String name  = nameIDMap.get(ID);
        if(name != null ) { return name;}

        val run = new QueryRunner();
        val stub = run.query(connection, "SELECT p.name FROM rule_definition p WHERE p.id = ?", new BeanHandler<StringStub>(StringStub.class), ID);

        if(stub != null ) {
            nameIDMap.put(ID, stub.getName());
            return stub.getName();
            }
        else {
            return ID;
        }
    }


    
    public static String findPersonPersonByID(final Connection connection, final String ID) throws SQLException {
        PersonStub stub  = personMap.get(ID);
        if(stub != null ) { return stub.toString().replace(',', '|');}

        val run = new QueryRunner();
        stub = run.query(connection, "SELECT p.id, p.first_name, p.middle_name, p.last_name FROM person_person p WHERE p.id = ?", new BeanHandler<PersonStub>(PersonStub.class), ID);

        if(stub != null ) {
                personMap.put(ID, stub);
                return stub.toString().replace(',', '|');
            }
        else {
            return ID;
        }
    }


    
    public static String findUserDefinedRoleTaskNameByID(final Connection connection, final String ID) throws SQLException {
        String name  = nameIDMap.get(ID);
        if(name != null ) { return name;}

        val run = new QueryRunner();
        val stub = run.query(connection, "SELECT p.name FROM permissionsuserdefinedrole p WHERE p.id = ?", new BeanHandler<StringStub>(StringStub.class), ID);

        if(stub != null ) {
                nameIDMap.put(ID, stub.getName());
                return stub.getName();
            }
        else {
            return ID;
        }
    }

    
    public static String findBjondTaskNameByID(final Connection connection, final String ID) throws SQLException {
        String name  = nameIDMap.get(ID);
        if(name != null ) { return name;}

        val run = new QueryRunner();
        val stub = run.query(connection, "SELECT p.name FROM bjondtask p WHERE p.id = ?", new BeanHandler<StringStub>(StringStub.class), ID);

        if(stub != null ) {
                nameIDMap.put(ID, stub.getName());
                return stub.getName();
            }
        else {
            return ID;
        }
    }

    
    public static String findAssessmentNameByID(final Connection connection, final String ID) throws SQLException {
        String name  = nameIDMap.get(ID);
        if(name != null ) { return name;}

        val run = new QueryRunner();
        val stub = run.query(connection, "SELECT p.name FROM assessment p WHERE p.id = ?", new BeanHandler<StringStub>(StringStub.class), ID);

        if(stub != null ) {
                nameIDMap.put(ID, stub.getName());
                return stub.getName();
            }
        else {
            return ID;
        }
    }

    
    public static String findRoleNameByID(final Connection connection, final String ID) throws SQLException {
        String name  = nameIDMap.get(ID);
        if(name != null ) { return name;}

        val run = new QueryRunner();
        val stub = run.query(connection, "SELECT p.name FROM roletypeentity p WHERE p.id = ?", new BeanHandler<StringStub>(StringStub.class), ID);

        if(stub != null ) {
                nameIDMap.put(ID, stub.getName());
                return stub.getName();
            }
        else {
            return ID;
        }
    }
    

    public static String findGroupNameByID(final Connection connection, final String ID) throws SQLException {
        String name  = nameIDMap.get(ID);
        if(name != null ) { return name;}

        val run = new QueryRunner();
        val stub = run.query(connection, "SELECT p.name FROM grouptypeentity p WHERE p.id = ?", new BeanHandler<StringStub>(StringStub.class), ID);

        if(stub != null ) {
                nameIDMap.put(ID, stub.getName());
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
