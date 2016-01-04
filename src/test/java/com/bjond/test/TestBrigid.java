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

package com.bjond.test;

import scala.ScalaHelloWorld;
import static org.assertj.core.api.StrictAssertions.assertThat;

import java.io.File;

import org.junit.Assert;
import org.junit.Test;

import lombok.val;

/** JUnit Test Suite TestBrigid
 *
 * @version 0.001 10/16/15
 * @author Stephen Agneta
 * @since Build 1.000
 *
 */

public class TestBrigid 
{
 
    /////////////////////////////////////////////////////////////////////////
    //                      Unit Tests below this point                    //
    /////////////////////////////////////////////////////////////////////////

    
    @Test
    public void sanityCheck() throws Exception {
        Assert.assertTrue("I ran ok!", true);
        System.out.println("This is a test"); // You should see this in the html report in stdout.


        // Try to find test data
        val resource = new File("./src/main/resources/secure-audit.log");
        Assert.assertTrue("I found test data", resource.exists());
        assertThat(resource.exists()).isTrue();

        assertThat(new ScalaHelloWorld().helloWorld()).isEqualTo("Hello World");
        System.out.println(" " + new ScalaHelloWorld().helloWorld());
    }


    
}

