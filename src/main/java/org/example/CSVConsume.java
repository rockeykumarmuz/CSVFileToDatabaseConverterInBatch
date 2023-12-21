package org.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class CSVConsume {

    public static void main(String[] args) {
        String jdbcUrl = "jdbc:mysql://localhost:3306/ems";
        String username = "root";
        String password = "123456";

        String filePath = "C:\\Users\\rocke\\Downloads\\sampleDatas.csv";

        int batchSize = 100;

        Connection connection = null;

        try {
            connection = DriverManager.getConnection(jdbcUrl, username, password);
            connection.setAutoCommit(false);

            String sql = "insert into employee(sr, name, gender, age, date, country) values(?,?,?,?,?,?)";

            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            BufferedReader lineReader = new BufferedReader(new FileReader(filePath));

            String lineText = null;

            int count = 0;

            lineReader.readLine();
            while ((lineText=lineReader.readLine())!=null) {
                String data[] = lineText.split(",");

                String sr = (data[0]);
                String name = data[1];
                String gender = data[2];
                String age = (data[3]);
                String date = data[4];
                String country = data[5];

                preparedStatement.setString(1, sr);
                preparedStatement.setString(2, name);
                preparedStatement.setString(3, gender);
                preparedStatement.setString(4, age);
                preparedStatement.setString(5, date);
                preparedStatement.setString(6, country);

                preparedStatement.addBatch();

                if(count%batchSize==0) {
                    preparedStatement.executeBatch();
                }
            }

            lineReader.close();
            preparedStatement.executeBatch();
            connection.commit();
            connection.close();
            System.out.println("Data has been inserted successfully");

        } catch(Exception e) {
            e.printStackTrace();
        }

    }

}
