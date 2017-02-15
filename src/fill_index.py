#! /usr/bin/env python
#
import argparse
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



def get_args():
    """
    Add command specific options
    """
    # initiate parser and fill args
    parser = argparse.ArgumentParser(description='Opal fill_index command line.')
    parser.add_argument('--opal', '-o', required=True, help='Opal server base url')
    parser.add_argument('--user', '-u', required=True, help='User name')
    parser.add_argument('--password', '-p', required=True, help='User password')
    parser.add_argument('ws', help='Web service path, for instance: /datasource/xxx/table/yyy/variable/vvv',default='/datasources')
    args = parser.parse_args()
    args.json = True
    args.method = 'GET'
    args.verbose = False
    return args


#get all args
args = get_args()

output_str = do_rest(args)


output_dict = json.loads(output_str)

print output_dict


# do the PUT
args.method = 'PUT'


if isinstance(output_dict,list): # all datasources
    for ds in output_dict :
       ds_link = ds['link']
       for tbl in ds['table']:
            ws_url = ds_link + '/table/' + uquote(tbl) + '/variables/_order'
            args.ws = ws_url
            res_output = do_rest(args)


elif isinstance(output_dict,dict): # a specific datasource
    ds = output_dict
    ds_link = args.ws
    for tbl in ds['table']:
        ws_url = ds_link + '/table/'+ uquote(tbl)+'/variables/_order'
        args.ws = ws_url
        res_output = do_rest(args)

