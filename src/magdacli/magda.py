'''

Magda client, build ontop of the generic APIClient

    MAGDA API Documentation

    This API documentation is for the open data platform MAGDA. REST is used. Unless stated otherwise, the JSON data format is used for all request and response body.  # noqa: E501

    OpenAPI spec version: 2.3.3

'''


from pyrest.rest import ApiClient
from pyrest.configuration import Configuration

import os, jwt , sys,re
from urllib.parse import urlparse

_is_illegal_header_value = re.compile(r'\n(?![ \t])|\r(?![ \t\n])').search
_is_illegal_header_value_bytes = re.compile(br'\n(?![ \t])|\r(?![ \t\n])').search


def _createToken(user, token):
    return jwt.encode(payload={"userId": user},key=token)

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
                                                                                        "value":_createToken(authid,authtoken)}
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
        
        # to allow partial override 
        if auth_settings is None:
            authkeys = self.configuration.auth_settings_map.keys()
            if not header_params is None:
                auth_settings = []
                
                for k in authkeys:
                    if not k in header_params:
                        auth_settings.append(k)
            else:
                auth_settings = authkeys
                
        #auth_settings=self.configuration.auth_settings_map.keys() if auth_settings is None else auth_settings
        
        result = super().call_api(resource_path, method, 
                                  path_params, query_params, header_params, 
                                  body, post_params, files, response_type, 
                                  auth_settings, 
                                  async_req, False, collection_formats, 
                                  _preload_content, _request_timeout, _raise_error)
        # if _raise_error is true this will be ignored
        # _return_http_data_only is ignored for downstream but used here to identify if we want errors returned
        if not self.replyOK( result[1] ):
            print(f"{resource_path}:{result[1]} - {result[0]}", file=sys.stderr, flush=True)
            #print(result[0], file=sys.stderr, flush=True)
            if not _return_http_data_only:
                return result
            return None
        return  result[0]
        

        
    
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
        if not isinstance(aspectlist,(tuple,list)):
            aspectlist = [aspectlist]   
        res = self.call_api(f"records",self.GET,query_params={"aspect":aspectlist},**kwargs)  
        records = None
        if "records" in res:
            records = res["records"]
            while "hasMore" in res and res["hasMore"]:
                npage = res["nextPageToken"]
                res = self.call_api(f"records",self.GET,query_params={"aspect":aspectlist,"pageToken":npage},**kwargs)  
                if "records" in res:
                    records.extend(["records"])
        return records
        
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
        recordsData =
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
    
    api_prefix = "/v0/"
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
        if ManagementMagdaClient.__instance == None:
            if "jwt-token" in apiprops:
                ManagementMagdaClient.__instance = ManagementMagdaClient(apiprops["url"],
                                                                 os.path.expandvars(apiprops["jwt-token"]),
                                                                 os.path.expandvars(apiprops.get("user-id","")),
                                                                 tenantId = apiprops.get("tenant-id",'0'),
                                                                 asjwt= True)
                
                
            elif "api-key" in apiprops:
                ManagementMagdaClient.__instance = ManagementMagdaClient(apiprops["url"],
                                                                         os.path.expandvars(apiprops["api-key"]),
                                                                         os.path.expandvars(apiprops["api-key-id"]))
            
        return ManagementMagdaClient.__instance
    
    
    
    # or Bearer [API Key ID]:[API key]
    def __init__(self,url,authtoken= None, authid= None, tenantId = '0', asjwt = False):
        
        
        
        self.configuration = Configuration()
        
        
        if asjwt:
            self.__internal = True
            self.configuration.auth_settings_map[ManagementMagdaClient.api_jwt_id] = {'in':"header","key":ManagementMagdaClient.api_jwt_id,
                                                                                        "value":_createToken(authid,authtoken)}
            self.configuration.auth_settings_map[ManagementMagdaClient.api_tenant_id] = {'in':"header","key":ManagementMagdaClient.api_tenant_id,
                                                                                         "value":tenantId}
        
        else:
            self.__internal = False
            self.configuration.auth_settings_map[ManagementMagdaClient.api_auth_id_name] = {'in':"header","key":ManagementMagdaClient.api_auth_id_name,
                                                                                        "value":authid}
            self.configuration.auth_settings_map[ManagementMagdaClient.api_auth_key_name] = {'in':"header","key":ManagementMagdaClient.api_auth_key_name,
                                                                                         "value":authtoken}
        self._baseUrl = "{}{}".format(url,ManagementMagdaClient.api_prefix)
        
        self._authprefix = "public" if self.__internal else "auth"
        
        '''
        
        if jwtoken:
            self.configuration.auth_settings_map[ManagementMagdaClient.api_auth_key_name] = {'in':"header","key":ManagementMagdaClient.api_jwt_id,"value":jwtoken}
        elif authtoken and authid:
            self.configuration.auth_settings_map[ManagementMagdaClient.api_auth_id_name] = {'in':"header","key":ManagementMagdaClient.api_auth_id_name,"value":authid}
            self.configuration.auth_settings_map[ManagementMagdaClient.api_auth_key_name] = {'in':"header","key":ManagementMagdaClient.api_auth_key_name,"value":authtoken}
        '''

        self.configuration.host = self._baseUrl
        self.configuration.verify_ssl = False
        #'content-type': 'application/json; charset=utf-8'
        # this is the default ,header_name='content-type', header_value='application/json; charset=utf-8')
        ApiClient.__init__(self, self.configuration)
        
        '''
        --header 'Content-Type: application/json' \
        '''
        
    def call_api(self, resource_path, method,
                 path_params=None, query_params=None, header_params=None,
                 body=None, post_params=None, files=None,
                 response_type=object, auth_settings=None, async_req=None,
                 _return_http_data_only=False, collection_formats=None,
                 _preload_content=True, _request_timeout=None, _raise_error= True):
        # _raise_error true means that we will raise an error on any non 2xx status!!
        
        # to allow partial override
        if auth_settings is None:
            authkeys = self.configuration.auth_settings_map.keys()
            if not header_params is None:
                
                auth_settings = []
                
                for k in authkeys:
                    if not k in header_params:
                        auth_settings.append(k)
            else:
                auth_settings = authkeys
                
        #auth_settings=self.configuration.auth_settings_map.keys() if auth_settings is None else auth_settings
        

        result = super().call_api(resource_path, method, 
                                  path_params, query_params, header_params, 
                                  body, post_params, files, response_type, 
                                  auth_settings, 
                                  async_req, False, collection_formats, 
                                  _preload_content, _request_timeout, _raise_error)
        
        # if _raise_error is true this will be ignored
        #if not 200 <= result[1] < 299 :
        if not self.replyOK( result[1] ):
            print(f"{resource_path}:{result[1]} - {result[0]}", file=sys.stderr, flush=True)
            return None
        return  result[0]
        
        
    def orgRootCreate(self, data ,**kwargs):
        return self.call_api(f"{self._authprefix}/orgunits/root",self.POST, body=data,**kwargs)

    def orgCreate(self, paid, data ,**kwargs):
        return self.call_api(f"{self._authprefix}/orgunits/{paid}/insert",self.POST, body=data,**kwargs)

        
    def orgGet(self, nodeId ,**kwargs):
        return self.call_api(f"{self._authprefix}/orgunits/{nodeId}",self.GET,**kwargs)
        
    def orgAllChildren(self, nodeId ,**kwargs):
        return self.call_api(f"{self._authprefix}/orgunits/{nodeId}/children/all",self.GET,**kwargs)
        
    def orgFromTo(self, lowerNodeId,higherNodeId, **kwargs):
        return self.call_api(f"{self._authprefix}/orgunits/{higherNodeId}/topDownPathTo/{lowerNodeId}",self.GET,**kwargs)
        
    def orgImmediateChildren(self, nodeId ,**kwargs):
        return self.call_api(f"{self._authprefix}/orgunits/{nodeId}/children/immediate",self.GET,**kwargs)
        
    def orgRoot(self ,**kwargs):
        return self.call_api(f"{self._authprefix}/orgunits/root",self.GET,**kwargs)
        
    def orgByLevel(self, orgLevel ,**kwargs):
        return self.call_api(f"{self._authprefix}/orgunits/bylevel/{orgLevel}",self.GET,**kwargs)
    
    def orgUpdate(self, nodeId,data ,**kwargs):
        return self.call_api(f"{self._authprefix}/orgunits/{nodeId}",self.PUT, body=data,**kwargs)

    
    def resourceGet(self,rid, **kwargs):
        return self.call_api(f"{self._authprefix}/resources/{rid}",self.GET,**kwargs)
    
    def resourceByURI(self,uri, **kwargs):
        return self.call_api(f"{self._authprefix}/resources/byUri/{uri}",self.GET,**kwargs)
    
    def resourceQuery(self,querymap, **kwargs):
        return self.call_api(f"{self._authprefix}/resources",self.GET,query_params=querymap, auth_settings=self.configuration.auth_settings_map.keys(),**kwargs)
    
    
    def whoami(self ,**kwargs):
        return self.call_api(f"{self._authprefix}/users/whoami",self.GET,**kwargs)
    
    
    def userGet(self, userId ,**kwargs):
        return self.call_api(f"{self._authprefix}/users/{userId}",self.GET,**kwargs)
    
    def userGetPermissions(self, userId ,**kwargs):
        return self.call_api(f"{self._authprefix}/users/{userId}/permissions",self.GET,**kwargs)
    
        
    def user4Role(self, roleId ,**kwargs):
        return self.call_api(f"{self._authprefix}/roles/{roleId}/users",self.GET,**kwargs)
    
    
    def userCount(self, nodeId ,**kwargs):
        return self.call_api(f"{self._authprefix}/users/count",self.GET,**kwargs)
    
    '''
    clarify  - seems odd
    def userGetRoles(self, userId ,**kwargs):
        return self.call_api(f"{self._authprefix}/users/{userId}/roles",self.GET,**kwargs)
    
    
    def userAddRoles(self, userId,roleIds ,**kwargs):
        return self.call_api(f"{self._authprefix}/users/{userId}/roles",self.POST,body=roleIds,**kwargs)  
    '''
    def roleAdd(self,name,description="" ,**kwargs):
        return self.call_api(f"{self._authprefix}/roles",self.POST,body={"name":name,"description":description},**kwargs)  
    
    
    
    def roleCreate(self,role, permissions,**kwargs):
        newrole = self.roleAdd(role["name"],role.get("description",""),**kwargs)
        for perm in permissions:
            self.permission2Role(newrole['id'], perm)
        
        return newrole
        
    def roleUpdate(self,roleId, name,description="" ,**kwargs):
        return self.call_api(f"{self._authprefix}/roles/{roleId}",self.PUT,body={"name":name,"description":description},**kwargs)
        
    
    def roleDelete(self,roleId ,**kwargs):
        return self.call_api(f"{self._authprefix}/roles/{roleId}", self.DELETE)
    
    
    def roleGet(self,roleId ,**kwargs):
        return self.call_api(f"{self._authprefix}/roles/{roleId}", self.GET,**kwargs)
    
    
    def roleQuery(self ,query={},**kwargs):
        return self.call_api(f"{self._authprefix}/roles", self.GET,query_params=query,**kwargs)
    
    def roleCount(self ,query={},**kwargs):
        return self.call_api(f"{self._authprefix}/roles/count", self.GET,query_params=query,**kwargs)
        
    
    def roleGetPermissions(self,roleId,query={},**kwargs):
        return self.call_api(f"{self._authprefix}/roles/{roleId}/permissions", self.GET,query_params=query,**kwargs)
      
        
    def permissionGet(self,permId,**kwargs):
        return self.call_api(f"{self._authprefix}/permissions/{permId}", self.GET,**kwargs)
        
    def permissionCount(self ,query={},**kwargs):
        return self.call_api(f"{self._authprefix}/permissions/count", self.GET,query_params=query,**kwargs)


    def permission2Role(self,roleId,record):
        '''
        role example
          "name": "a test permission",
          "user_ownership_constraint": false,
          "org_unit_ownership_constraint": true,
          "pre_authorised_constraint" : false,
          "allow_exemption": false,
          "description": "a test permission",
           "resourceUri": "object/dataset/draft",
          "operationUris": ["object/dataset/draft/read", "object/dataset/draft/write"]
          
        '''
        return self.call_api(f"{self._authprefix}/roles/{roleId}/permissions", self.POST,body=record)
        
    
    def permissionDeleteRole(self,roleId,permId):
        return self.call_api(f"{self._authprefix}/roles/{roleId}/permissions/{permId}", self.DELETE)
    
    def permissionEditRole(self,roleId,permId, record):
        return self.call_api(f"{self._authprefix}/roles/{roleId}/permissions/{permId}", self.PUT,body=record)
    
    def roleCopy(self,roleId, newName):
        role = self.roleGet(roleId)
        #newrole = self.roleAdd(newName, role["description"])
        newrole= {"name":newName,"description": role["description"]}
        permissions = stripPermissions(self.roleGetPermissions(roleId))
        
        newrole = self.roleCreate(newrole,permissions)
        """
        
        for perm in permissions:
            '''
            del perm["resource_id"]
            del perm["id"]
            del perm["create_by"]
            del perm["create_time"]
            del perm["edit_by"]
            del perm["edit_time"]
            
            perm["operationUris"] = [op["uri"] for op in perm["operations"]]
            perm["resourceUri"] = perm["resource_uri"]
            del perm["operations"]
            del perm["resource_uri"]
            '''
            self.permission2Role(newrole['id'], perm)

        """
        permissions = self.roleGetPermissions(newrole['id'])
        return {"role":newrole,"permissions":permissions}
        
    '''
    This is no longer aacessible, use registry
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
    '''
    def encode(self, context,key):
        return encodeKey(context, key)
        
    
    def opa(self, path = '', jdata = None, query = None, headers = None, **kwargs):
        if len(path) > 0:
            if path[0] != '/':
                path = '/'+path
                
        return self.call_api(f"opa/decision{path}", self.POST,body=jdata, query_params=query, **kwargs)
    
        
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


def stripPermissions(permissions):
    npermissions = []
    for perm in permissions:
        del perm["resource_id"]
        del perm["id"]
        del perm["create_by"]
        del perm["create_time"]
        del perm["edit_by"]
        del perm["edit_time"]

        perm["owner_id"] = None
        perm["operationUris"] = [op["uri"] for op in perm["operations"]]
        perm["resourceUri"] = perm["resource_uri"]
        del perm["operations"]
        del perm["resource_uri"]
            
        npermissions.append(perm)
            
    return npermissions

def encodeSession(session,tenant_id=0):
    return {ManagementMagdaClient.api_jwt_id:session,ManagementMagdaClient.api_tenant_id:tenant_id}

'''
def encodeAuthSettings(session,tenant_id=0):
    return {ManagementMagdaClient.api_jwt_id:{"in":"header","key":ManagementMagdaClient.api_jwt_id,"value":session},
            "X-Magda-Tenant-Id":{"key":"X-Magda-Tenant-Id","value":tenant_id}}
'''         

def getSession(headers):
    args =  headers.get(ManagementMagdaClient.api_jwt_id,None),headers.get(ManagementMagdaClient.api_tenant_id,0)
    for a in args:
        if a is None or _is_illegal_header_value(a):
            raise ValueError("Invalid session for Magda client!")
    
    return args
    
def simplifyAspectList(aspectdata):
    # the aspects are packed up into separate 'data' in a list, lift them up as key/dict 
    aspects = {}
    for aspectd in aspectdata:
        aspects[aspectd["id"]] = aspectd["data"]
        
    return aspects

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
                        or a  jwt-token that take s precedence 
                        the rest base URL without path "url" 
    
    '''
    return ManagementMagdaClient.getInstance(apiprops)

    
    