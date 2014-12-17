import java.io.*;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.Locale;

public class JDBC2CSV {

    public static void main(String[] args) {

        if (args.length < 5) {
            System.err.println("java -jar SQLServer2CSV.jar <target file|-> <sql to exectute> {-v <variable>} <connect string> <uid> <pwd> [<split file by lines>]");
            System.err.println("target file = - means return the result to console (only 1 col)");
            System.err.println("");
            System.err.println("Sample:");
            System.err.println("java -jar SQLServer2CSV.jar C:\test.csv example.sql localhost username password 1000000");
            System.exit(255);
        }

        Boolean exception = false;
        ArrayList<String> variables = new ArrayList<String>();
        String outputFilePath = null;
        String inputFilePath = null;
        String connStr = null;
        String connUser = null;
        String connPassword = null;
        Integer lineCount = 0;
        Boolean alwaysQuoteNotNumeric = true;

        int argCnt = 0;
        for (int argi = 0; argi < args.length; ++argi) {
            if (args[argi].equalsIgnoreCase("-v")) {
                if (++argi < args.length) {
                    variables.add(args[argi]);
                }
                continue;
            }
            switch (argCnt) {
                case 0:
                    outputFilePath = args[argi];
                    break;
                case 1:
                    inputFilePath = args[argi];
                    break;
                case 2:
                    connStr = args[argi];
                    break;
                case 3:
                    connUser = args[argi];
                    break;
                case 4:
                    connPassword = args[argi];
                    break;
                case 5:
                    lineCount = Integer.parseInt(args[argi]);
                    break;
            }
            argCnt++;
        }

        String filePrefix;
        String fileSuffix;
        if (outputFilePath.lastIndexOf(".") > 0) {
            filePrefix = outputFilePath.substring(0, outputFilePath.lastIndexOf("."));
            fileSuffix = outputFilePath.substring(outputFilePath.lastIndexOf("."));
        } else {
            filePrefix = outputFilePath;
            fileSuffix = "";
        }

        /* Set Locale */
        Locale.setDefault(Locale.ENGLISH);

        String decimalSeparator = ".";
        String delimiter = ";";
        String quote = "\"";
        String doubleQuote = quote + quote;

        String sqlQueury = "";
        FileInputStream inputFileStream = null;
        BufferedInputStream bufferInputFileStream = null;

        try {

            File inputFile = new File(inputFilePath);
            byte[] buffer = new byte[(int) inputFile.length()];

            if (buffer.length < inputFile.length()) {
                System.out.println("Input file is too long");
                throw new Exception();
            }

            inputFileStream = new FileInputStream(inputFile);
            bufferInputFileStream = new BufferedInputStream(inputFileStream);
            bufferInputFileStream.read(buffer);
            sqlQueury = new String(buffer);
        } catch (Exception e) {
            System.out.println("Could not handle input file: " + inputFilePath);
            e.printStackTrace(System.err);
            exception = true;
        } finally {
            if (bufferInputFileStream != null) {
                try {
                    bufferInputFileStream.close();
                } catch (IOException e) {
                    e.printStackTrace(System.err);
                    exception = true;
                }
            }
            if (inputFileStream != null) {
                try {
                    inputFileStream.close();
                } catch (IOException e) {
                    e.printStackTrace(System.err);
                    exception = true;
                }
            }
            if (exception) {
                System.exit(255);
            }
        }

        /* Remove ; from end of SQL string if it exists */
        sqlQueury = sqlQueury.replaceFirst("; *$", "");

        File outputFile = null;
        BufferedWriter bufferOutputWriter = null;
        FileWriter outputFileWriter = null;
        String outputFileCounter = "A";
        Integer lineCounter = 1;

        Connection con = null;
        PreparedStatement statement = null;
        try {
            if (outputFilePath.equals("-")) {
                bufferOutputWriter = new BufferedWriter(new OutputStreamWriter(System.out));
            } else {
                outputFile = new File(filePrefix + ((lineCount > 0) ? outputFileCounter : "") + fileSuffix);
                outputFileWriter = new FileWriter(outputFile);
                bufferOutputWriter = new BufferedWriter(outputFileWriter);
            }
            con = (Connection) DriverManager.getConnection(connStr, connUser, connPassword);
            con.setAutoCommit(false);

            statement = (PreparedStatement) con.prepareStatement(sqlQueury);
            
            Integer varCount = 1;
            for (String v : variables) {
                statement.setString(varCount++, v);
            }

            statement.setFetchSize(100);
            ResultSet resultSet = (ResultSet) statement.executeQuery();

            /* Get result set metadata */
            ResultSetMetaData rsMetaData = resultSet.getMetaData();
            int numberOfColumns = rsMetaData.getColumnCount();
            String currValue;
            if (outputFilePath.equals("-")) {
                if (numberOfColumns > 1) {
                    System.out.println("Error: 1 field only when output goes to -");
                    System.exit(255);
                } else if (resultSet.next() == false) {
                    System.out.print("");
                    System.exit(0);
                } else {
                    System.out.print(resultSet.getString(1));
                    System.exit(0);
                }
            }
            // Write header
            for (int i = 1; i <= numberOfColumns; i++) {
                currValue = rsMetaData.getColumnName(i);
                bufferOutputWriter.write(quote + currValue.replace(quote, doubleQuote) + quote);

                if (i < numberOfColumns) {
                    bufferOutputWriter.write(delimiter);
                }
            }

            bufferOutputWriter.write("\n");
            while (resultSet.next()) {
                // Create a new file if limit reached
                if (lineCount > 0 && lineCounter > 0 && (lineCounter % lineCount) == 0 && !outputFilePath.equals("-")) {
                    try {
                        bufferOutputWriter.close();
                    } catch (IOException e) {
                        e.printStackTrace(System.err);
                        exception = true;
                    }

                    // Close writer
                    if (outputFileWriter != null) {
                        try {
                            outputFileWriter.close();
                        } catch (IOException e) {
                            e.printStackTrace(System.err);
                            exception = true;
                        }
                    }
                    
                    // Find next Index
                    outputFileCounter = new String(Character.toChars(outputFileCounter.charAt(0) + 1));
                    
                    // Create new file
                    outputFile = new File(filePrefix + outputFileCounter + fileSuffix);
                    outputFileWriter = new FileWriter(outputFile);
                    bufferOutputWriter = new BufferedWriter(outputFileWriter);
                }

                // Write columns
                for (int i = 1; i <= numberOfColumns; i++) {
                    currValue = "";
                    if (rsMetaData.getColumnType(i) == java.sql.Types.NUMERIC) {
                        BigDecimal temp = resultSet.getBigDecimal(i);
                        if (temp != null) {
                            // Strip trailing zeros from numbers
                            currValue = temp.stripTrailingZeros().toPlainString();
                            // Add zero if string start with decimal separator
                            if (currValue.indexOf(decimalSeparator) == 0) {
                                currValue = '0' + currValue;
                            }
                        }
                    } else {
                        String temp = resultSet.getString(i);
                        if(temp != null) {
                            currValue = temp;
                        }
                    }
                    
                    // Escape delimiters and double quotes
                    if ((alwaysQuoteNotNumeric && rsMetaData.getColumnType(i) != java.sql.Types.NUMERIC) || currValue.contains(quote) || currValue.contains(delimiter) || currValue.contains("\n")) {
                        currValue = quote + currValue.replace(quote, doubleQuote) + quote;
                    }
                    
                    // Write to output
                    bufferOutputWriter.write(currValue);

                    // Add delimiter if not last value
                    if (i < numberOfColumns) {
                        bufferOutputWriter.write(delimiter);
                    }
                }
                // End of line
                bufferOutputWriter.write("\n");
                // Count it
                lineCounter++;
            }

        } catch (Exception e) {
            e.printStackTrace(System.err);
            exception = true;
        } finally {
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    e.printStackTrace(System.err);
                    exception = true;
                }
            }

            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace(System.err);
                    exception = true;
                }
            }

            if (bufferOutputWriter != null) {
                try {
                    bufferOutputWriter.close();
                } catch (IOException e) {
                    e.printStackTrace(System.err);
                    exception = true;
                }
            }
            if (outputFileWriter != null) {
                try {
                    outputFileWriter.close();
                } catch (IOException e) {
                    e.printStackTrace(System.err);
                    exception = true;
                }
            }
            if (!exception) {
                System.exit(0);
            } else {
                System.exit(255);
            }
        }
    }
}
