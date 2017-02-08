#! /usr/bin/env python
#
from argparse import Namespace
import opal.core
import sys
import pycurl
import json
from urllib import quote as uquote
#

def do_rest(args):
    """
    Execute REST command
    """
    # Build and send request
    try:

        request = opal.core.OpalClient.build(opal.core.OpalClient.LoginInfo.parse(args)).new_request()
        request.fail_on_error()

        if args.verbose:
            request.verbose()

        # send request
        request.method(args.method).resource(args.ws)
        response = request.send()

        # format response
        res = response.content
        if args.json:
            res = response.pretty_json()
        elif args.method in ['OPTIONS']:
            res = response.headers['Allow']

        # output to stdout
        #print res

        # return the result
        return res

    except Exception, e:
        print e
        sys.exit(2)
    except pycurl.error, error:
        errno, errstr = error
        print >> sys.stderr, 'An error occurred: ', errstr
        sys.exit(2)




args = Namespace(
    accept=None, content_type=None,json=False,
    method='GET',
    opal='https://opal-demo.obiba.org',
    password='password', user='administrator',
    verbose=False,
    ws='/datasource/HELIAD'
)


# opal rest -o https://opal-demo.obiba.org -u administrator -p password -m PUT /datasource/CLS/table/Wave1/variables/_order



print(args)

output_str = do_rest(args)

print (output_str)

print args.ws


output_dict = json.loads(output_str)

print '\n'

if isinstance(output_dict,list):
    for ds in output_dict :
       ds_link = ds['link']
       for tbl in ds['table']:
            ws_url = ds_link + '/table/' + uquote(tbl) + '/variables'
            args.ws = ws_url
            print '[REQUEST]: ' + str(args)
            res_output = do_rest(args)
            print '[RESULT]: ' + str(res_output) + '\n\n'

elif isinstance(output_dict,dict):
    ds = output_dict
    ds_link = args.ws
    for tbl in ds['table']:
        ws_url = ds_link + '/table/'+ uquote(tbl)+'/variables'
        args.ws = ws_url
        print '[REQUEST]: '+str(args)
        res_output = do_rest(args)
        print '[RESULT]: '+str(res_output) + '\n\n'