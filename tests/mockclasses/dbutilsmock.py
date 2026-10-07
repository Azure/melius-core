class fileInfomock:
    def __init__(self, path, size=100):
        self.path = path
        self.size = size
        pass


class fsmock:
    @staticmethod
    def mounts():
        pass

    @staticmethod
    def cp():
        pass

    @staticmethod
    def rm(path=None, recurse=None):
        pass

    @staticmethod
    def ls(path=None):
        pass

    @staticmethod
    def mv(source=None, destination=None):
        pass

    @staticmethod
    def mkdirs(path=None):
        pass


class entrypointmock:
    @staticmethod
    def getDbutils():
        return dbutilextended()


class dbutilextended:
    @staticmethod
    def notebook():
        return notebookextended()


class notebookextended:
    @staticmethod
    def getContext():
        return contextextended()


class contextextended:
    @staticmethod
    def toJson():
        return '{"rootRunId":null,"currentRunId":null,"jobGroup":"3392566265716830055_7169099258085469241_569970240b344e4f8156b5601d9b5548","tags":{"jobId": "10", "idInJob":"11", "opId":"ServerBackend-207410a0506bc5d4","opTarget":"com.databricks.backend.common.rpc.InternalDriverBackendMessages$StartRepl","clusterMemory":"890880","serverBackendName":"com.databricks.backend.daemon.driver.DriverCorral","notebookId":"82540922448576","projectName":"chauffeur","eventWindowTime":"228108.38499999954","httpTarget":"/notebook/82540922448576/command/675304162457768","buildHash":"854e9f0abc3c97e4712885bbf5c7536a27dfe32a","browserHash":"#notebook/82540922448576/command/1277508756699336","host":"10.139.64.100","notebookLanguage":"python","sparkVersion":"6.4.x-scala2.11","hostName":"0430-012240-lamp494-10-139-64-100","httpMethod":"POST","browserIdleTime":"2324","jettyRpcJettyVersion":"8","browserTabId":"9a907ae3-d6bc-46bb-8e7f-800b487e49a7","sourceIpAddress":"50.54.130.237","browserUserAgent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.85 Safari/537.36 Edg/90.0.818.46","orgId":"7070317340299782","userAgent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.85 Safari/537.36 Edg/90.0.818.46","clusterId":"0430-012240-lamp494","rootOpId":"ServiceMain-4dc8980b599b0002","sessionId":"8a6f6e2dbeeff60fdd9181f6f9d8a3e329d674a5236c9121cf35e4c904ab49ec","clusterCreator":"Webapp","clientBranchName":"3.44.4216","clusterType":"spot","browserHasFocus":"true","userId":"2725696411635308","browserIsHidden":"false","principalIdpId":"33e01921-4d64-4f8c-a055-5bdaffd5e33d","opType":"ServerBackend","sourcePortNumber":"0","user":"minkpark@ame.gbl","principalIdpObjectId":"c99ba01f-2be7-4f93-94af-a425c8ed4ff5","browserHostName":"adb-7070317340299782.2.azuredatabricks.net","parentOpId":"RPCClient-4dc8980b599b3a9c","jettyRpcType":"InternalDriverBackendMessages$DriverBackendRequest"},"extraContext":{"aclPathOfAclRoot":"/workspace/2579499173730732/2579499173730734/82540922448576","notebook_path":"/Users/minkpark@ame.gbl/test","api_url":"https://westus.azuredatabricks.net","api_token":"[REDACTED]"},"credentialKeys":["adls_aad_token","adls_gen2_aad_token"]}'


class notebookmock:
    entry_point = entrypointmock()


class widgetsmock:
    @staticmethod
    def text(param:str, default:str):
        pass

    @staticmethod
    def get(param:str):
        return param


class dbutilsmock:
    fs = fsmock()
    notebook = notebookmock()
    widgets = widgetsmock()

    def __init__(self) -> None:
        pass