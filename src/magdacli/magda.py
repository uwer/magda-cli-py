'''

Magda client, build ontop of the generic APIClient

    MAGDA API Documentation

    This API documentation is for the open data platform MAGDA. REST is used. Unless stated otherwise, the JSON data format is used for all request and response body.  # noqa: E501

    OpenAPI spec version: 2.3.3

'''


from pyrest.rest import ApiClient
from pyrest.configuration import Configuration

import os, jwt , sys
from urllib.parse import urlparse

class AspectMagdaClient(ApiClient):
    '''
    simple Magda client just managing the records and aspects associated with records 
    
    Utilises simple REST API
    '''
    
    api_prefix = "api/v0/registry"
    internal_prefix = "v0"
    api_auth_key_name = "X-Magda-API-Key"
    api_auth_id_name = "X-Magda-API-Key-Id"
    api_jwt_id = "X-Magda-Session"
    api_tenant_id = "X-Magda-Tenant-Id"
    
    
    __instance = None
    
    @staticmethod 
    def getInstance(apiprops):
        '''
         parameter - apiprops, dict with mandatory keys "api-key","api-key-id","url" 
                        for authentication "api-key","api-key-id"
                        the rest base URL without path "url" 
    
        '''
        if AspectMagdaClient.__instance == None:
            ## this is crude and this wants to come from a better place then an function argument 
            if "jwt-token" in apiprops:
                AspectMagdaClient.__instance = AspectMagdaClient(os.path.expandvars(apiprops["jwt-token"]),
                                                                 os.path.expandvars(apiprops.get("user-id","")),
                                                                 apiprops["url"],
                                                                 tenantId = apiprops.get("tenant-id",'0'),
                                                                 asjwt= True)
                
                
            else:
                AspectMagdaClient.__instance = AspectMagdaClient(os.path.expandvars(apiprops["api-key"]),os.path.expandvars(apiprops["api-key-id"]),apiprops["url"])
            
        
        
        return AspectMagdaClient.__instance
        
        
    
    # or Bearer [API Key ID]:[API key]
    def __init__(self,authtoken, authid, url, tenantId = '0', asjwt = False):
        
        purl = urlparse(url)
        
        self.configuration = Configuration()
        if asjwt:
            self.__internal = True
            api_prefix = AspectMagdaClient.internal_prefix
            self.configuration.auth_settings_map[AspectMagdaClient.api_jwt_id] = {'in':"header","key":AspectMagdaClient.api_jwt_id,
                                                                                        "value":self._createToken(authid,authtoken)}
            self.configuration.auth_settings_map[AspectMagdaClient.api_tenant_id] = {'in':"header","key":AspectMagdaClient.api_tenant_id,
                                                                                         "value":tenantId}
        
        else:
            self.__internal = False
            api_prefix = AspectMagdaClient.api_prefix
            self.configuration.auth_settings_map[AspectMagdaClient.api_auth_id_name] = {'in':"header","key":AspectMagdaClient.api_auth_id_name,
                                                                                        "value":authid}
            self.configuration.auth_settings_map[AspectMagdaClient.api_auth_key_name] = {'in':"header","key":AspectMagdaClient.api_auth_key_name,
                                                                                         "value":authtoken}
        
            
        self._baseUrl = "{}://{}:{}/{}/".format(purl.scheme,purl.netloc,purl.port if not purl.port is None else 80,api_prefix)
        
        self.configuration.host = self._baseUrl
        self.configuration.verify_ssl = False
        #'content-type': 'application/json; charset=utf-8'
        ApiClient.__init__(self, self.configuration)# this is the default ,header_name='content-type', header_value='application/json; charset=utf-8')
        
        '''
        --header 'Content-Type: application/json' \
        '''
        
        
    def call_api(self, resource_path, method,
                 path_params=None, query_params=None, header_params=None,
                 body=None, post_params=None, files=None,
                 response_type=object, auth_settings=None, async_req=None,
                 _return_http_data_only=True, collection_formats=None,
                 _preload_content=True, _request_timeout=None, _raise_error= False):
        
        auth_settings=self.configuration.auth_settings_map.keys() if auth_settings is None else auth_settings
        
        result = super().call_api(resource_path, method, 
                                  path_params, query_params, header_params, 
                                  body, post_params, files, response_type, 
                                  auth_settings, 
                                  async_req, False, collection_formats, 
                                  _preload_content, _request_timeout, _raise_error)
        # if _raise_error is true this will be ignored
        # _return_http_data_only is ignored for downstream but used here to identify if we want errors returned
        if not self.replyOK( result[1] ):
            print(result[0], file=sys.stderr, flush=True)
            if not _return_http_data_only:
                return result
            return None
        return  result[0]
        
    def _createToken(self,user, token):
        return jwt.encode(payload={"userId": user},key=token)
        
    
    def aspectCreate(self,aspectDict,**kwargs):        
        return self.call_api("aspects",self.POST, body=aspectDict,**kwargs)
        
    def aspectGetAll(self,**kwargs):        
        return self.call_api("aspects",self.GET,**kwargs)
        
        
    def aspectGet(self,aspectID,**kwargs):        
        return self.call_api(f"aspects/{aspectID}",self.GET,**kwargs)
        
    def aspectEdit(self,aspectID,aspectDict,**kwargs):        
        return self.call_api(f"aspects/{aspectID}",self.PUT, body=aspectDict,**kwargs)
        
        
    def aspectPatch(self,aspectID,aspectDict,**kwargs):        
        return self.call_api(f"aspects/{aspectID}",self.PATCH, body=aspectDict,**kwargs)
        
        
    def recordAspectDelete(self,recordId,aspectID,**kwargs):        
        return self.call_api(f"records/{recordId}/aspects/{aspectID}",self.DELETE,**kwargs)
        
    def recordAspectGet(self,recordId,aspectID,**kwargs):        
        return self.call_api(f"records/{recordId}/aspects/{aspectID}",self.GET,**kwargs)
        
    def recordAspectPATCH(self,recordId,aspectID,aspectDataDict,**kwargs):        
        # inconclusive from API if query or body
        return self.call_api(f"records/{recordId}/aspects/{aspectID}",self.PATCH, body=aspectDataDict,**kwargs)
    
    def recordAspectPATCHMultiple(self,aspectID,aspectAndRecordIdDataDict,**kwargs):        
        # inconclusive from API if query or body
        return self.call_api(f"records/aspects/{aspectID}",self.PATCH, body=aspectAndRecordIdDataDict,**kwargs)
    
        
    def recordAspectPut(self,recordId,aspectID,aspectDataDict,**kwargs):        
        return self.call_api(f"records/{recordId}/aspects/{aspectID}",self.PUT, body=aspectDataDict,**kwargs)
        

    def recordAspectGetAll(self,recordId,**kwargs):        
        return self.call_api(f"records/{recordId}/aspects",self.GET,**kwargs)  
    
    def recordAspectGetByAspect(self,aspectlist,**kwargs):        
        return self.call_api(f"records",self.GET,query_params={"aspect":aspectlist},**kwargs)  
        
    def recordAspectGetCount(self,recordId,**kwargs):        
        return self.call_api(f"records/{recordId}/aspects/count",self.GET,**kwargs)        


    def recordGet(self,recordId,**kwargs):        
        return self.call_api(f"records/inFull/{recordId}",self.GET,**kwargs)
        
    def recordGetPagetokens(self,**kwargs):        
        return self.call_api(f"records/pagetokens",self.GET,**kwargs)    
    
    def recordGetSummaries(self,**kwargs):        
        return self.call_api(f"records/summary",self.GET,**kwargs)#)    
        
    def recordGetSummary(self,recordId,**kwargs):        
        return self.call_api(f"records/summary/{recordId}",self.GET,**kwargs)  
        
        
    def recordCreate(self,recordsData,**kwargs):        
        return self.call_api(f"records",self.POST,  body=recordsData,**kwargs)
        
        
    def recordPatch(self,recordId,recordsData,**kwargs):        
        return self.call_api(f"records/{recordId}",self.PATCH,body=recordsData,**kwargs)
        
    def recordPatchMultiple(self, recordsData,**kwargs):       
        '''
        {
          "recordIds": "Unknown Type: string[]",
          "jsonPatch": "Unknown Type: object[]"
        }
        ''' 
        return self.call_api(f"records/",self.PATCH, body=recordsData,**kwargs)
        
        
    def recordEdit(self,recordId,recordsData,**kwargs):        
        return self.call_api(f"records/{recordId}",self.PUT,body=recordsData,**kwargs)
        
        
    def recordDelete(self,recordId,**kwargs):        
        return self.call_api(f"records/{recordId}",self.DELETE,**kwargs)
        
        
    def testAuth(self,**kwargs):
        if not self.__internal:
            return self.call_api(f"auth/users/whoami",self.GET,**kwargs)
        
        return "not available with jwt token authentication"
        
        
    def searchRecordByName(self,searchOrg, exact=True):
        org = str(searchOrg).lower()
        res = self.recordGetSummaries()
        #print(json.dumps(res))
        rec = None
        
        if exact:
            while res["hasMore"]:
                for rec in res["records"]:
                    if org == rec["name"].lower():
                        #print(json.dumps(rec))
                        return rec
                res = self.recordGetSummaries(query_params={"pageToken":res["nextPageToken"]})
            
        else:
            org = org.replace(" ", "")
            while res["hasMore"]:
                for rec in res["records"]:
                    if org in rec["name"].lower().replace(" ", ""):
                        #print(json.dumps(rec))
                        return rec
                res = self.recordGetSummaries(pageToken=res["nextPageToken"])
                
        return None
    
    
    def encode(self, context,key):
        return encodeKey(context, key)
    
    
class ManagementMagdaClient(ApiClient):
    '''
    simple Magda client just managing the records and aspects associated with records 
    
    Utilises simple REST API
    '''
    
    api_prefix = "/api/v0/"
    api_auth_key_name = "X-Magda-API-Key"
    api_auth_id_name = "X-Magda-API-Key-Id"
    
    __instance = None
    
    @staticmethod 
    def getInstance(apiprops):
        '''
         parameter - apiprops, dict with mandatory keys "api-key","api-key-id","url" 
                        for authentication "api-key","api-key-id"
                        the rest base URL without path "url" 
    
        '''
        if ManagementMagdaClient.__instance == None:
            ## this is crude and this wants to come from a better place then an function argument 
            ManagementMagdaClient.__instance = ManagementMagdaClient(apiprops["api-key"],apiprops["api-key-id"],apiprops["url"])
        
        
        return ManagementMagdaClient.__instance
    
    # or Bearer [API Key ID]:[API key]
    def __init__(self,authtoken, authid, url):
        
        self._baseUrl = "{}{}".format(url,ManagementMagdaClient.api_prefix)
        self.configuration = Configuration()
        self.configuration.auth_settings_map[ManagementMagdaClient.api_auth_id_name] = {'in':"header","key":ManagementMagdaClient.api_auth_id_name,"value":authid}
        self.configuration.auth_settings_map[ManagementMagdaClient.api_auth_key_name] = {'in':"header","key":ManagementMagdaClient.api_auth_key_name,"value":authtoken}
        #configuration.api_key["X-Magda-API-Key-Id"] = authid
        #configuration.api_key["X-Magda-API-Key"] = authtoken
        self.configuration.host = self._baseUrl
        self.configuration.verify_ssl = False
        #'content-type': 'application/json; charset=utf-8'
        ApiClient.__init__(self, self.configuration)# this is the default ,header_name='content-type', header_value='application/json; charset=utf-8')
        
        '''
        --header 'Content-Type: application/json' \
        '''
        
    def call_api(self, resource_path, method,
                 path_params=None, query_params=None, header_params=None,
                 body=None, post_params=None, files=None,
                 response_type=object, auth_settings=None, async_req=None,
                 _return_http_data_only=False, collection_formats=None,
                 _preload_content=True, _request_timeout=None, _raise_error= True):
        
        auth_settings=self.configuration.auth_settings_map.keys() if auth_settings is None else auth_settings

        result = super().call_api(resource_path, method, 
                                  path_params, query_params, header_params, 
                                  body, post_params, files, response_type, 
                                  auth_settings, 
                                  async_req, False, collection_formats, 
                                  _preload_content, _request_timeout, _raise_error)
        
        # if _raise_error is true this will be ignored
        if not 200 <= result[1] < 299 :
            print(result[0])
            return None
        return  result[0]
        
        
    def orgRootCreate(self, data ,**kwargs):
        return self.call_api(f"auth/orgunits/root",self.POST, body=data,**kwargs)

    def orgCreate(self, paid, data ,**kwargs):
        return self.call_api(f"auth/orgunits/{paid}/insert",self.POST, body=data,**kwargs)

        
    def orgGet(self, nodeId ,**kwargs):
        return self.call_api(f"auth/orgunits/{nodeId}",self.GET,**kwargs)
        
    def orgAllChildren(self, nodeId ,**kwargs):
        return self.call_api(f"auth/orgunits/{nodeId}/children/all",self.GET,**kwargs)
        
    def orgFromTo(self, lowerNodeId,higherNodeId, **kwargs):
        return self.call_api(f"auth/orgunits/:{higherNodeId}/topDownPathTo/:{lowerNodeId}",self.GET,**kwargs)
        
    def orgImmediateChildren(self, nodeId ,**kwargs):
        return self.call_api(f"auth/orgunits/{nodeId}/children/immediate",self.GET,**kwargs)
        
    def orgRoot(self ,**kwargs):
        return self.call_api(f"auth/orgunits/root",self.GET,**kwargs)
        
    def orgByLevel(self, orgLevel ,**kwargs):
        return self.call_api(f"auth/orgunits/bylevel/{orgLevel}",self.GET,**kwargs)
    
    def orgUpdate(self, nodeId,data ,**kwargs):
        return self.call_api(f"auth/orgunits/:{nodeId}",self.PUT, body=data,**kwargs)

    
    def resourceGet(self,rid, **kwargs):
        return self.call_api(f"auth/resources/{rid}",self.GET,**kwargs)
    
    def resourceByURI(self,uri, **kwargs):
        return self.call_api(f"auth/resources/byUri/{uri}",self.GET,**kwargs)
    
    def resourceQuery(self,querymap, **kwargs):
        return self.call_api(f"auth/resources",self.GET,query_params=querymap, auth_settings=self.configuration.auth_settings_map.keys(),**kwargs)
    
    def resourceCreate(self, data, **kwargs):
        return self.call_api(f"admin/resources/", self.POST,body=data,**kwargs)

    def resourceUpdate(self, rid, data, **kwargs):
        return self.call_api(f"admin/resources/{rid}", self.PUT,body=data,**kwargs)
        
    
    def connectorsAll(self, **kwargs):
        return self.call_api(f"admin/connectors",self.GET,**kwargs)
        
    
    
    def connectorUpdate(self, cid, data, **kwargs):
        return self.call_api(f"admin/connectors/{cid}", self.PUT, body=data,**kwargs)
    
    
    def connectorCreate(self, data, **kwargs):
        return self.call_api(f"admin/connectors/",self.POST, body=data,**kwargs)
    
    def encode(self, context,key):
        return encodeKey(context, key)
        
    
    
def encodeKey(context,key):
    cnxt = context.lower()
    if cnxt == 'entity':
        if key.startswith('org-'):
            return key
        return f"org-{key}"
    
    if cnxt == 'resource':
        if key.startswith('ds-'):
            return key
        return f"ds-{key}"
    
    if cnxt == 'dataset':
        if key.startswith('ds-'):
            return key
        return f"ds-{key}"
    
    if 'distr' in cnxt :
        if key.startswith('dist-'):
            return key
        return f"dist-{key}"
    
    if cnxt == 'method':
        if key.startswith('methd-'):
            return key
        return f"methd-{key}"

    
    return key
   
    
def createRegistryClient(apiprops):
    '''
    This call creates a singelton, so its ok to call repeatedly
    It supports multiple parallel requests 
    parameter - apiprops, dict with mandatory keys "api-key","api-key-id","url" 
                        for authentication "api-key","api-key-id"
                        the rest base URL without path "url"
                        
        or for internal connections 
                dict with mandatory keys "jwt-key","user-id","url" 
                        for authentication "jwt-key","user-id"
                        the rest base URL without path "url"
         
    '''
    return AspectMagdaClient.getInstance(apiprops)


def createManagmentClient(apiprops):
    '''
    This call creates a singelton, so its ok to call repeatedly
    It supports multiple parallel requests 
    parameter - apiprops, dict with mandatory keys "api-key","api-key-id","url" 
                        for authentication "api-key","api-key-id"
                        the rest base URL without path "url" 
    
    '''
    return ManagementMagdaClient.getInstance(apiprops)

    
    