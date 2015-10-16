# brigid
Accepts an audit log in stdin and emits an un-opaqued version to stdout.
Brigid is the Celtic god of knowledge and wisdom


# To build the jar file at root of project execute the following command without arguments.
gradle

# To clean the application
gradle clean

# To run the application...
java -jar ./build/libs/brigid.jar < ~/tmp/secure-audit.log  > humanreadable.log

# To run tests
gradle test

# Test result html is viewable at:
./build/reports/tests/classes/com.bjond.test.TestBrigid.html

