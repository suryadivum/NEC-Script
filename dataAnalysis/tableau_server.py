import tableauserverclient as tsc
from confidential import Confidential
import requests

tableau_auth = tsc.TableauAuth(
    Confidential.user_name,
    Confidential.user_password,
    site_id='')


server = tsc.Server(
    Confidential.server_address,
    use_server_version=True)


try:
    with server.auth.sign_in(tableau_auth):
        all_workbooks_items, pagination_item = server.workbooks.get()
        wb_id = all_workbooks_items[0]
        # print(wb_id)
        metadata_endpoint = f'https://{Confidential.server_address}/api/metadata/graphql'
        # all_projects, pagination = server.projects.get()
        # wb_item = tsc.WorkbookItem(all_projects[1].id)
        query = '''
                    query {
                      workbooks(filter: { id: { eq: "%s" } }) {
                        upstreamDatasources {
                          name
                          connectionType
                        }
                      }
                    }
        ''' % wb_id
        metadata_response = requests.post(metadata_endpoint, json={'query': query})
        data_source_name = metadata_response.json()['data']['workbooks'][0]['upstreamDatasources'][0]['name']
        print(data_source_name)
        # for project in all_projects:
        #     print(f'Project Name: {project.name}, Project ID: {project.id}')
        # print(type(all_projects))
        # wb_id = server.workbooks.publish(workbook_item=wb_item, file='./Test2.twbx', mode='Overwrite')
        # print(wb_item)
        # all_views, pagination_item = server.views.get()
        # for v in all_views:
        #     name = v.name
        #     server.views.populate_image(v)
        #     with open(f'./{name}.png', 'wb') as f:
        #         f.write(v.image)
        #     server.views.populate_csv(v)
        #     with open(f'./{name}.csv', 'wb') as f:
        #         f.write(b''.join(v.csv))
        #     print(v.name, end="\n")
        print("Success")
    server.auth.sign_out()
except Exception as e:
    print(str(e))
