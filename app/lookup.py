import pymysql.cursors
import sys
from credentials import user, password
from pprint import pprint

def _create_connection(dbname):
    return pymysql.connect(host=dbname + '.analytics.db.svc.eqiad.wmflabs',
                           user=user,
                           password=password,
                           db=dbname + '_p',
                           charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)


def _run_query(dbname, q, fetchall=False):
    connection = _create_connection(dbname)
    with connection.cursor() as cursor:
        cursor.execute(q)
        if fetchall is True:
            result = cursor.fetchall()
        else:
            result = cursor.fetchone()
        return result


def _normalize_page_title(page_title):
    return page_title[0].upper() + page_title[1:].replace(' ', '_')


def _find_title(page_title, page_namespace, dbname):
    page_title = _normalize_page_title(page_title)
    q = ('select count(*) from page where page_namespace = {0} and '
         'page_title = "{1}" and page_is_redirect = 0;').format(
             page_namespace, page_title)
    result = _run_query(dbname, q)
    result = int(result['count(*)'])
    if result == 0:
        return False
    elif result == 1:
        return True


def _get_external_link_count(dbname):
    namespace = 0
    if dbname == 'commonswiki':
        namespace = 6  # File namespace, the de-facto "main" NS of Commons
    q = ('select count(*) from external_links join page on page_id = el_from '
        'where page_namespace = {0};').format(str(namespace))
    result = _run_query(dbname, q)
    return int(result)


def get_all_wikis():
    return [x['dbname'] for x in
            _run_query('meta', 'select dbname from wiki;', True)]


def get_all_external_link_counts(all_wikis):
    manifest = []
    for dbname in all_wikis:
        count = _get_external_link_count(dbname)
        manifest.append((dbname, count))
    return manifest


def find_page_on_all_wikis(page_title, page_namespace, all_wikis):
    page_title = _normalize_page_title(page_title)
    manifest = []
    for dbname in all_wikis:
        page_exists = _find_title(page_title, page_namespace, dbname)
        if page_exists is True:
            manifest.append(dbname)
    return manifest


def get_title_and_namespace(page_title_with_ns):
    page_title_with_ns = _normalize_page_title(page_title_with_ns)
    parts = page_title_with_ns.split(':')
    namespaces = {'Talk': 1,
                  'User': 2,
                  'User_talk': 3,
                  'Project': 4,
                  'Project_talk': 5,
                  'File': 6,
                  'File_talk': 7,
                  'MediaWiki': 8,
                  'MediaWiki_talk': 9,
                  'Template': 10,
                  'Template_talk': 11,
                  'Help': 12,  # i need somebody
                  'Help_talk': 13,  # not just anybody
                  'Category': 14,
                  'Category_talk': 15,
                  'Portal': 100,
                  'Portal_talk': 101,
                  'Book': 108,
                  'Book_talk': 109,
                  'Draft': 118,
                  'Draft_talk': 119,
                  'Education_Program': 446,
                  'Education_Program_talk': 447,
                  'TimedText': 710,
                  'TimedText_talk': 711,
                  'Module': 828,
                  'Module_talk': 829,
                  'Gadget': 2300,
                  'Gadget_talk': 2301,
                  'Gadget_definition': 2302,
                  'Gadget_definition_talk': 2303}
    if parts[0] in namespaces:
        return _normalize_page_title(':'.join(parts[1:])), namespaces[parts[0]]
    else:
        return page_title_with_ns, 0


if __name__ == '__main__':
    all_wikis = get_all_wikis()
    page_input = sys.argv[1]
    if page_input == '!externallinks':
        all_el_counts = get_all_external_link_counts(all_wikis)
        for resultpair in all_el_counts:
            print('{0}: {1}'.format(resultpair[0], str(resultpair[1])))
    else:
        page_title, page_namespace = get_title_and_namespace(page_input)
        print('Looking up {0} in namespace {1} on {2} wikis'.format(
            page_title, str(page_namespace), str(len(all_wikis))))
        matches = find_page_on_all_wikis(page_title, page_namespace, all_wikis)
        for match in matches:
            print(match)
