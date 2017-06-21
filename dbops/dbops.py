import pymysql
import logging
from datetime import datetime

_db = None
def init(host, db, user, password, force=False):
    global _db
    if(_db is None or force):
        _db = pymysql.connect(host=host, \
                              db=db, \
                              user=user, \
                              password=password, \
                              charset='utf8mb4')
        logging.info('DB host: %s | DB database: %s | DB user: %s' % (host, db, user))

def _query(query, *values):
    global _db
    assert _db is not None, 'No database connection established, must run init() first'

    cursor = _db.cursor()
    
    if(len(values) == 0):
        cursor.execute(query)
        logging.info('Query: %s' % query)
    else:
        cursor.execute(query, values)
        logging.info('Query: %s | Values: %s' % (query, str(values)))

    return cursor

def _commit():
    global _db
    assert _db is not None, 'No database connection established, must run init() first'
    _db.commit()
    logging.info('DB committed')

def _query_commit(query, *values):
    _query(query, *values) 
    _commit()

_epoch = datetime(1970, 1, 1)
def get_current_timestamp():
    global _epoch
    return int((datetime.utcnow() - _epoch).total_seconds())

#############
# Companies #
#############

def insert_company(name, google_dev_id=None, company_type=None):
    company_type = 'dev' if google_dev_id is not None and company_type is None else company_type

    query = """INSERT INTO companies(googleDevId, commonName, type)
               VALUES (%s, %s, %s)
               ON DUPLICATE KEY UPDATE
               commonName=%s, type=%s"""
    cursor = _query_commit(query, google_dev_id, name, company_type, name, company_type)

    # Get the primary key of the new row
    query = """SELECT id FROM companies
               WHERE commonName=%s"""
    cursor = _query(query, name)
    (pkey,) = cursor.fetchone()

    logging.info('Added %d (%s) to companies table' % (pkey, name))

    return pkey 

#####################
# Apps and releases #
#####################

def insert_app(dev_key, package_name, common_name, product_url=None, last_checked=None, icon_url=None, install_count=0, run_status=0, is_family=0):
    # Set the timestamp to the current UTC time if not provided
    if(last_checked is None):
        last_checked = get_current_timestamp()

    query = """INSERT INTO apps(packageName, commonName, devCompanyId, productUrl, timestampLastChecked, iconUrl, installCount, runStatus, isFamily)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON DUPLICATE KEY UPDATE
               packageName=%s, commonName=%s, devCompanyId=%s, productUrl=%s, timestampLastChecked=%s, iconUrl=%s, installCount=%s, runStatus=%s, isFamily=%s"""
    cursor = _query_commit(query, package_name, common_name, dev_key, product_url, last_checked, icon_url, install_count, run_status, is_family, \
                                  package_name, common_name, dev_key, product_url, last_checked, icon_url, install_count, run_status, is_family)

    # Get the primary key of the new row
    query = """SELECT id FROM apps
               WHERE packageName=%s"""
    cursor = _query(query, package_name)
    (pkey,) = cursor.fetchone()

    logging.info('Added %d (%s) to apps table' % (pkey, package_name))

    return pkey

def update_app_check_time(package_name, last_checked=None):
    # Set the timestamp to the current UTC time if not provided
    if(last_checked is None):
        last_checked = get_current_timestamp()

    query = """UPDATE apps
               SET timestampLastChecked=%s
               WHERE packageName=%s"""
    cursor = _query_commit(query, last_checked, package_name)

    # Get the primary key of the new row
    query = """SELECT id FROM apps
               WHERE packageName=%s"""
    cursor = _query(query, package_name)
    (pkey,) = cursor.fetchone()

    logging.info('Updated check time to %d for %d (%s)' % (last_checked, pkey, package_name))

    return pkey

def update_app_run_status(package_name, run_status):
    # Only 4 valid values for run_status:
    # -1 - ERROR
    # 0 - available for testing
    # 1 - currently testing
    # 2 - logs to process
    assert run_status in [-1, 0, 1, 2], 'Invalid run status value %d, must be 0 (available), 1 (testing), or 2 (processing)' % run_status

    query = """UPDATE apps
               SET runStatus=%s
               WHERE packageName=%s"""
    cursor = _query_commit(query, run_status, package_name)

    logging.info('Updated run status to %d for package %s' % (run_status, package_name))
    
def update_app_icon(package_name, icon_url):
    logging.warn('update_app_icon() is meant for testing purposes only')
    query = """UPDATE apps
               SET iconUrl=%s
               WHERE packageName=%s"""
    cursor = _query_commit(query, icon_url, package_name)

    logging.info('Updated iconUrl for %s' % package_name)

def update_family(package_name, is_family):
    logging.warn('update_family() is meant for testing purposes only')
    query = """UPDATE apps
               SET isFamily=%s
               WHERE packageName=%s"""
    cursor = _query_commit(query, is_family, package_name)

    logging.info('Updated family flag to %d for %s' % (is_family, package_name))

def update_app_install_count(package_name, install_count):
    logging.warn('update_app_install_count() is meant for testing purposes only')
    query = """UPDATE apps
               SET installCount=%s
               WHERE packageName=%s"""
    cursor = _query_commit(query, install_count, package_name)

    logging.info('Updated installCount for %s to %d' % (package_name, install_count))

def insert_app_release(app_key, version_code, version_string, timestamp_publish, \
                       timestamp_download=None, has_iap=None, has_ads=None, social_nets=None, tested=0):
    query = """INSERT INTO appReleases(appId, versionCode, versionString, timestampPublish, timestampDownload, hasInAppPurchases, hasAds, socialNetworks, tested)
               VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON DUPLICATE KEY UPDATE
               appId=%s, versionCode=%s, versionString=%s, timestampPublish=%s, timestampDownload=%s, hasInAppPurchases=%s, hasAds=%s, socialNetworks=%s, tested=%s"""
    cursor = _query_commit(query, \
                           app_key, version_code, version_string, timestamp_publish, timestamp_download, has_iap, has_ads, social_nets, tested, \
                           app_key, version_code, version_string, timestamp_publish, timestamp_download, has_iap, has_ads, social_nets, tested)

    # Get the primary key of the new row
    query = """SELECT id FROM appReleases
               WHERE appId=%s AND versionCode=%s"""
    cursor = _query(query, app_key, version_code)
    (pkey,) = cursor.fetchone()

    logging.info('Added %d (%d) to appReleases table' % (pkey, version_code))

    return pkey

def update_release_tested(release_key, tested=True):
    query = """UPDATE appReleases
               SET tested=%s
               WHERE id=%s"""
    cursor = _query_commit(query, tested, release_key)
    logging.info('Marked release id %d as tested=%s' % (release_key, tested))

    if(tested):
        query = """UPDATE apps
                   SET runStatus=0
                   WHERE id IN
                        (SELECT appId FROM appReleases
                         WHERE id=%s)"""
        cursor = _query_commit(query, release_key)
        logging.info('Marked app for release id %d as runStatus=0' % release_key)

##############
# Categories #
##############

def insert_categories(app_key, categories):
    if(len(categories) > 0):
        # Put any new categories in the table
        cat_values = ','.join(['(%s)' for cat in categories])
        query = """INSERT IGNORE INTO categories(categoryName)
                   VALUES %s""" % cat_values
        cursor = _query_commit(query, *categories)
        logging.info('Added %s to categories table' % str(categories))

        # Get the keys for this app's categories
        condition = ' OR '.join(['categoryName=%s' for cat in categories])
        query = """SELECT id FROM categories
                   WHERE %s""" % condition
        cursor = _query(query, *categories)
        new_cat_keys = [key[0] for key in cursor]
        logging.info('App %d has current category keys %s' % (app_key, str(new_cat_keys)))

        # Get the category keys already associated for this app
        query = """SELECT categoryId FROM appCategoriesMapping
                   WHERE appId=%s"""
        cursor = _query(query, app_key)
        old_cat_keys = [key[0] for key in cursor]
        logging.info('App %d has old category keys %s' % (app_key, str(old_cat_keys)))

        # Remove any category mappings that no longer apply
        remove_cat_keys = list(set(old_cat_keys) - set(new_cat_keys))
        if(len(remove_cat_keys) > 0):
            condition = ' OR '.join(['categoryId=%s' for cat_key in remove_cat_keys])
            query = """DELETE FROM appCategoriesMapping
                       WHERE appId=%d AND (%s)""" % (app_key, condition)
            cursor = _query_commit(query, *remove_cat_keys)
            logging.info('App %d lost old category keys %s' % (app_key, str(remove_cat_keys)))

        # Add any new category mappings
        add_cat_keys = list(set(new_cat_keys) - set(old_cat_keys))
        if(len(add_cat_keys) > 0):
            cat_values = ','.join(['(%d,%s)' % (app_key, '%s') for cat in add_cat_keys])
            query = """INSERT IGNORE INTO appCategoriesMapping(appId, categoryId)
                       VALUES %s""" % cat_values
            cursor = _query_commit(query, *add_cat_keys)
            logging.info('App %d gained new category keys %s' % (app_key, str(add_cat_keys)))

####################
# Test permissions #
####################

def insert_permission(release_id, permission, timestamp, is_used=False, tester_id=None):
    query = """INSERT INTO testPermissions(timestamp, releaseId, permission, isUsed, testerId)
               VALUES(%s, %s, %s, %s, %s)
               ON DUPLICATE KEY UPDATE
               timestamp=%s, releaseId=%s, permission=%s, isUsed=%s, testerId=%s"""
    cursor = _query_commit(query, \
                           timestamp, release_id, permission, is_used, tester_id, \
                           timestamp, release_id, permission, is_used, tester_id)

    logging.info('Added permission %s (%s) for release ID %d' % (permission, is_used, release_id))

def clear_permission_results(release_id):
    query = """DELETE FROM testPermissions
               WHERE releaseId=%s"""
    cursor = _query_commit(query, release_id)
    logging.info('Removed all permissions results for release ID %d' % release_id)

######################
# Test transmissions #
######################

def insert_transmission(release_id, data_type, timestamp, \
                        domain=None, tls_sni=None, ip_address=None, port=None, is_tls=False, payload=None, tester_id=None):
    query = """INSERT INTO testTransmissions(timestamp, releaseId, domain, tlsSNI, ipAddress, port, isTLS, dataType, payload)
               VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON DUPLICATE KEY UPDATE
               timestamp=%s, releaseId=%s, domain=%s, tlsSNI=%s, ipAddress=%s, port=%s, isTLS=%s, dataType=%s, payload=%s"""
    cursor = _query_commit(query, \
                           timestamp, release_id, domain, tls_sni, ip_address, port, is_tls, data_type, payload, \
                           timestamp, release_id, domain, tls_sni, ip_address, port, is_tls, data_type, payload)

    logging.info('Added data transmission of type %s to domain %s (%s) for release Id %d' % (data_type, domain, ip_address, release_id))

def clear_transmission_results(release_id):
    query = """DELETE FROM testTransmissions
               WHERE releaseId=%s"""
    cursor = _query_commit(query, release_id)
    logging.info('Removed all transmissions results for release ID %d' % release_id)

###########
# Getters #
###########

def get_app_id(package_name):
    query = """SELECT id FROM apps
               WHERE packageName=%s"""
    cursor = _query(query, package_name)

    try:
        (app_id,) = cursor.fetchone()
        logging.info('Found appId %d for package %s' % (app_id, package_name))
        return app_id
    except TypeError:
        logging.warning('Found no appId for package %s' % package_name)
        return None

def get_release_id(package_name, version_code):
    query = """SELECT id FROM appReleases
               WHERE appId IN
                    (SELECT id FROM apps
                     WHERE packageName=%s)
               AND versionCode=%s"""
    cursor = _query(query, package_name, version_code)
    
    try:
        (release_id,) = cursor.fetchone()
        logging.info('Found releaseId %d for package %s version %d' % (release_id, package_name, version_code))
        return release_id
    except TypeError:
        logging.warning('Found no releaseId for package %s version %d' % (package_name, version_code))
        return None

def get_app_to_test():
    query = """SELECT packageName,
                      MAX(appReleases.versionCode),
                      installCount,
                      priority
                      FROM `apps`
                      INNER JOIN appReleases ON apps.id=appReleases.appId AND appReleases.tested=0
                      WHERE runStatus=0
                      GROUP BY packageName, installCount, priority
                      ORDER BY priority DESC, installCount DESC
                      LIMIT 1"""
    cursor = _query(query)

    try:
        (package_name, version_code, install_count, priority) = cursor.fetchone()
        logging.info('Found package %s version code %d (priority %d, %d installs)' % (package_name, version_code, priority, install_count))
        return (package_name, version_code)
    except TypeError:
        logging.warning('Found no apps to test from the database')
        return None

def is_app_in_db(package_name, version_code):
    return get_release_id(package_name, version_code) is not None

###################
# Standalone test #
###################

if __name__ == '__main__':
    logging.basicConfig(level=20)
    init('localhost', 'appcensus', 'appcensus', 'placeholder')

    (app1, vc1) = get_app_to_test()
    print(app1)
    update_app_run_status(app1, 1)
    (app2, vc2) = get_app_to_test()
    print(app2)

