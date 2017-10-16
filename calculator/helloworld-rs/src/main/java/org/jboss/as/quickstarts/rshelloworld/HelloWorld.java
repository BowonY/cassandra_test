/*
 * JBoss, Home of Professional Open Source
 * Copyright 2015, Red Hat, Inc. and/or its affiliates, and individual
 * contributors by the @authors tag. See the copyright.txt in the
 * distribution for a full listing of individual contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jboss.as.quickstarts.rshelloworld;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.SQLException;

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;

/**
 * A simple REST service which is able to say hello to someone using HelloService Please take a look at the web.xml where JAX-RS
 * is enabled
 *
 * @author gbrey@redhat.com
 *
 */

@Path("/")
public class HelloWorld {
    static String[] CONTACT_POINTS = {"172.31.12.10","172.31.13.100", "172.31.5.57"};
//    static String[] CONTACT_POINTS = {"127.0.0.1"};
    static int PORT = 9042;
    Cluster cluster;
    Session session;
    Connection connection;
    Statement statement;
    public static HashMap additionTable = new HashMap();
    public static HashMap subtractionTable = new HashMap();
    public static HashMap multiplicationTable = new HashMap();
    public static HashMap divisionTable = new HashMap();
    public static HashMap[] tables =
            {additionTable, subtractionTable, multiplicationTable, divisionTable};


    public enum Calculation {
        add("Addition"),
        subtract("Subtraction"),
        multiply("Multiplication"),
        divide("Division");
        private String tablename;

        Calculation(String tablename) {
            this.tablename = tablename;
        }

        String getTableName() {
            return tablename;
        }

        double calculate(double val1, double val2) {
            switch(this) {
                case add:
                    return val1+val2;
                case subtract:
                    return val1-val2;
                case multiply:
                    return val1*val2;
                case divide:
                    if (val2 == 0)
                        throw new AssertionError("Val2 can't be zero");
                    return val1/val2;
                default:
                    throw new AssertionError("Unknown calculation");
            }
        }
        HashMap getTableCache() {
            switch(this) {
                case add:
                    return additionTable;
                case subtract:
                    return subtractionTable;
                case multiply:
                    return multiplicationTable;
                case divide:
                    return divisionTable;
                default:
                    throw new AssertionError("Unknown calculation");
                }
        }

    }

    public class Key {
        private final double val1;
        private final double val2;
        public Key(double val1, double val2) {
            this.val1 = val1;
            this.val2 = val2;
        }
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Key)) return false;
            Key tuple = (Key) o;
            return val1 == tuple.val1 && val2 == tuple.val2;
        }
        @Override
        public int hashCode() {
            return 0;
        }
        public String toString() {
            return String.format("%f, %f", val1, val2);
        }
    }

    public HelloWorld() throws ClassNotFoundException {
//        for (HashMap m: tables) {
//                Iterator it = m.entrySet().iterator();
//            while (it.hasNext()) {
//                Map.Entry pair = (Map.Entry)it.next();
//                System.out.println(pair.getKey().toString() + " = " + pair.getValue());
//            }
//            System.out.println("\nabc\n");
//        }
        try {
            // for MySQL
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println("class not found!!");
        }
    }

    public void connectCassandra() {
        cluster = Cluster.builder()
                .addContactPoints(CONTACT_POINTS).withPort(PORT)
                .build();
        session = cluster.connect("log");
        System.out.println("connection successsss!!");
    }

    public void connectMysql() throws SQLException {
        try {
            // for MySQL
//            connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/log", "server","server");
            connection = DriverManager.getConnection("jdbc:mysql://172.31.12.10:3306/log", "server","server");
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("connection failed!!");
        }
        statement = connection.createStatement();
        System.out.println("connection successsss!!");
    }

    @Inject
    HelloService helloService;

    @GET
    @Path("/cache/cassandra/{function}/{value1}/{value2}")
    @Produces({ "application/json" })
    public String getCacheCassandraCalculation(
                              @PathParam("function") String function,
                              @PathParam("value1") double value1,
                              @PathParam("value2") double value2) {
         //
        connectCassandra();
        long startTime = System.currentTimeMillis();
        Calculation cal = Calculation.valueOf(function);
        String tablename = cal.getTableName();
        //look up cache
        Key key = new Key(value1, value2);
        Object data = cal.getTableCache().get(key);
//        String query = String.format("SELECT result FROM log." + tablename
//            + " WHERE val1 = %f AND val2 = %f", value1, value2);
//        System.out.println(query);
//        ResultSet rs = session.execute(query);
//        Row row = rs.one();
        //java.sql.ResultSet rs = statement.executeQuery(query);
        double answer;
        String query, response;
        if (data == null) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
            }
            answer = cal.calculate(value1, value2);
            query = String.format("INSERT INTO log." + tablename
               + "(val1, val2, result) "
               + "VALUES (%f, %f, %f)", value1, value2, answer);
            System.out.println(query);
            //statement.executeUpdate(query);
            session.execute(query);
            cal.getTableCache().put(key, answer);
            response = "new";
        }
        else {
//            answer = row.getDouble(0);
            //answer = rs.getDouble(1);
            answer = (double) data;
            System.out.println(answer);
            response = "old";
        }
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        System.out.println(elapsedTime);
        cluster.close();
        return "{\"result\":\"" + response + "\"}";
}

    @GET
    @Path("/cache/mysql/{function}/{value1}/{value2}")
    @Produces({ "application/json" })
    public String getCacheMysqlCalculation(
                              @PathParam("function") String function,
                              @PathParam("value1") double value1,
                              @PathParam("value2") double value2) {
         //
        try {
        connectMysql();
        } catch (SQLException e) {
            System.out.println("connection error");
        }
        long startTime = System.currentTimeMillis();
        Calculation cal = Calculation.valueOf(function);
        String tablename = cal.getTableName();
        //look up cache
        Key key = new Key(value1, value2);
        Object data = cal.getTableCache().get(key);
//        String query = String.format("SELECT result FROM log." + tablename
//           + " WHERE val1 = %f AND val2 = %f", value1, value2);
//        System.out.println(query);
//        ResultSet rs = session.execute(query);
//        Row row = rs.one();
        //java.sql.ResultSet rs = statement.executeQuery(query);
        double answer;
        String query, response;
        if (data == null) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
            }
            answer = cal.calculate(value1, value2);
            query = String.format("INSERT INTO log." + tablename
               + "(val1, val2, result) "
               + "VALUES (%f, %f, %f)", value1, value2, answer);
            System.out.println(query);
            try {
                statement.executeUpdate(query);
            } catch (SQLException e) {
                System.out.println("sqlexception error");
            }
            //session.execute(query);
            cal.getTableCache().put(key, answer);
            response = "new";
        }
        else {
//            answer = row.getDouble(0);
            //answer = rs.getDouble(1);
            answer = (double) data;
            System.out.println(answer);
            response = "old";
        }
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        System.out.println(elapsedTime);
        return "{\"result\":\"" + response + "\"}";
}

    @GET
    @Path("/cassandra/{function}/{value1}/{value2}")
    @Produces({ "application/json" })
    public String getCassandraCalculation(@PathParam("function") String function,
                              @PathParam("value1") double value1,
                              @PathParam("value2") double value2) {
        //
        connectCassandra();
        long startTime = System.currentTimeMillis();
        Calculation cal = Calculation.valueOf(function);
        String tablename = cal.getTableName();
        //
        String query = String.format("SELECT result FROM log." + tablename
            + " WHERE val1 = %f AND val2 = %f", value1, value2);
        System.out.println(query);
        ResultSet rs = session.execute(query);
        Row row = rs.one();
        //java.sql.ResultSet rs = statement.executeQuery(query);
        double answer;
        String response;
        if (row == null) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
            }
            answer = cal.calculate(value1, value2);
            query = String.format("INSERT INTO log." + tablename
               + "(val1, val2, result) "
               + "VALUES (%f, %f, %f)", value1, value2, answer);
            System.out.println(query);
            //statement.executeUpdate(query);
            session.execute(query);
            response = "new";
        }
        else {
            answer = row.getDouble(0);
            //answer = rs.getDouble(1);
            System.out.println(answer);
            response = "old";
        }
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        System.out.println(elapsedTime);
        return "{\"result\":\"" + response + "\"}";
    }

    @GET
    @Path("/mysql/{function}/{value1}/{value2}")
    @Produces({ "application/json" })
    public String getMysqlCalculation(@PathParam("function") String function,
                              @PathParam("value1") double value1,
                              @PathParam("value2") double value2)
                              throws SQLException {
        //
        connectMysql();
        long startTime = System.currentTimeMillis();
        Calculation cal = Calculation.valueOf(function);
        String tablename = cal.getTableName();
        //
        String query = String.format("SELECT result FROM log." + tablename
            + " WHERE val1 = %f AND val2 = %f", value1, value2);
        System.out.println(query);
        //ResultSet rs = session.execute(query);
        java.sql.ResultSet rs = statement.executeQuery(query);
        double answer;
        String response;
        if (!rs.next()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
            }
            answer = cal.calculate(value1, value2);
            query = String.format("INSERT INTO log." + tablename
               + "(val1, val2, result) "
               + "VALUES (%f, %f, %f)", value1, value2, answer);
            System.out.println(query);
            statement.executeUpdate(query);
            response = "new";
        }
        else {
            answer = rs.getDouble(1);
            System.out.println(answer);
            response = "old";
        }
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        System.out.println(elapsedTime);
        return "{\"result\":\"" + response + "\"}";
    }

    @GET
    @Path("/reset/mysql")
    @Produces({ "application/json" })
    public String resetMysql() {
        try {
            connectMysql();
            statement.executeQuery("TRUNCATE log.Addition");
            statement.executeQuery("TRUNCATE log.Subtraction");
            statement.executeQuery("TRUNCATE log.Multiplication");
            statement.executeQuery("TRUNCATE log.Division");
        }
        catch (SQLException e) {
            System.out.println("connection problem");
        }
        for (HashMap m: tables) {
                Iterator it = m.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry)it.next();
                //System.out.println(pair.getKey().toString() + " = " + pair.getValue());
                it.remove();
            }
            //System.out.println("\nabc\n");
        }
        return "{\"result\":\"" + "reset "+ "\"}";
    }

    @GET
    @Path("/reset/cassandra")
    @Produces({ "application/json" })
    public String resetCassandra() {
        connectCassandra();
        session.execute("TRUNCATE log.Addition");
        session.execute("TRUNCATE log.Subtraction");
        session.execute("TRUNCATE log.Multiplication");
        session.execute("TRUNCATE log.Division");
        for (HashMap m: tables) {
                Iterator it = m.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry)it.next();
                //System.out.println(pair.getKey().toString() + " = " + pair.getValue());
                it.remove();
            }
            //System.out.println("\nabc\n");
        }

        return "{\"result\":\"" + "reset "+ "\"}";
    }
}
