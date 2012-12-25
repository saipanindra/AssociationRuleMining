The arm.java mines the given instance of transactions (from items.dat and trans.dat)
and writes out the results to four different files.

The program takes the input from a file written in the following format
username = <yourusername>, password = <password>
TASK1: support = 6%
TASK2: support = 7%
TASK3: support = 7%, size = 3
TASK4: support = 2%, confidence = 95%, size = 3


The username and password are your credentials on the Oracle db instance you
are running.
Referring to the above file sample, 

TASK1 prompts for all the items in the basket(transaction instance)
that have a support of atleast 6% (Check out System.out.1 file for results after the you execute the program)

TASK2 prompts for all the items in the basket(transaction instance)
that have a support of atleast 7% . Now the item set can be of size 1 or 2.
(Results in System.out.2 after the program is run.)

TASK3 prompts for all the items in the basket(transaction instance)
that have a support of atleast 3% . Now the item set can be of size <=3.
(Results in System.out.3 after the program is run.)

TASK4 prompts for all the Association Rules in the basket(transaction instance)
that have a support of atleast 2% ,confidence of 95%. The size of item set in the
association rule is <=3. (Results in System.out.4)


The other inputs for the program are items.dat and trans.dat which contain a sample
transaction instance and items list.

All the PL/SQL procedures and the views used are in the arm.sql file which should be in the
current folder that the program is being run in.

Do include the ojdbc5.jar in the classpath while you compile and run the program.
And yes, change the connection string to match your requirements after you search for the below line
Connection con = DriverManager.getConnection("jdbc:oracle:thin:hr/hr@oracle1.cise.ufl.edu:1521:orcl",uname.trim(), pword.trim());
