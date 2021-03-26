msg = {
            'meta': {},
            'dns': {},
            'http': {}
    }

msg['meta']['host'] = 'testhost'

msg['http']['start'] = 1
msg['http']['schema'] = 'https'
msg['http']['host'] = 'test'
msg['http']['path'] = '/'
msg['http']['url'] = 'test-url'
msg['http']['regex'] = '\dfdf'
msg['http']['regex_found'] = True
msg['http']['elapsed'] = 1
msg['http']['status_code'] = 1
msg['http']['reason'] = 'test code'


msg['dns']['start'] = 1
msg['dns']['elapsed'] = 2
msg['dns']['ip'] = '1.2.3.4'

table_name = 'testtable'
query = """
        INSERT INTO {} (
            host,
            dns_start,
            dns_elapsed,
            ip,
            http_start,
            http_elapsed,
            http_schema,
            http_url_root,
            http_path,
            http_url,
            http_regex,
            http_status_code,
            http_status_code_reason,
            http_retgex_found
        ) VALUES (
            '{meta[host]}',
            {dns[start]},
            {dns[elapsed]},
            '{dns[ip]}',
            {http[start]},
            {http[elapsed]},
            '{http[schema]}',
            '{http[host]}',
            '{http[path]}',
            '{http[url]}',
            '{http[regex]}',
            {http[status_code]},
            '{http[reason]}',
            {http[regex_found]}
        )
    """.format(
        table_name,
        **msg
    )

print("+++++++++++++++++++++++++++++++++++")
print(query.strip())
print("+++++++++++++++++++++++++++++++++++")
