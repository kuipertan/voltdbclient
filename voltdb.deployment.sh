<?xml version="1.0"?>
<deployment>
    <cluster hostcount="4" kfactor="0"  sitesperhost="16" />
    <httpd enabled="true"  port="8787" >
        <jsonapi enabled="true" />
    </httpd>
    <security enabled="true" />
    <users>
        <user name="admin" password="adminer" roles="administrator" />
        <user name="cloud" password="cloudrain" roles="user" />
        <user name="cloudu" password="c1oud" roles="user" />
        <user name="cloudp" password="c1oude2" roles="user" />
        <user name="bfder" password="bfder" roles="rdonly" />
        <user name="bfduser" password="bfduser1qaz" roles="role0" />
        <user name="bfdqa" password="bfdqazx" roles="role1" />
        <user name="bfdguest" password="bfdguest2wsx" roles="role2" />
    </users>
    <paths>
         <voltdbroot path="/opt/sys/voltdbpath/voltdbroot" />
         <exportoverflow path="/opt/sys/voltdbpath/overflow" />
    </paths>
    <systemsettings>
        <resourcemonitor frequency="15">
            <memorylimit size="180"/>
            <disklimit>
            <feature name="droverflow" size="30%"/>
            </disklimit>
        </resourcemonitor>
        <temptables maxsize='256' />
    </systemsettings>
    <heartbeat timeout="90" />

</deployment>
