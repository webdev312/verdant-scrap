import json
import requests
import MySQLdb

# API token
g_strToken = "769b49ae6b2fd4e6678d55cafd74f0c872969b68ed984d295096ddab0bb018b6"

# Global variables
g_arr_hotel = list()

def isNull(entry):
    if (entry == "None"): return "null"
    return entry.replace("’", "'")

def get_total_page_count(strlink):
    req_link = strlink + "&access_token=" + g_strToken
    resp = requests.get(req_link).json()
    last_page = resp['meta']['links']['last']
    last_page_num = last_page[last_page.find('&page=')+6:last_page.find('&perpage=')]
    return last_page_num

def get_hotel_list(cursor):
    try:
        n_pages = get_total_page_count("https://api.thermostatsolutions.com/v1/hotels/?page=0&perpage=1")

        strlinkHotel = "https://api.thermostatsolutions.com/v1/hotels/?page=0&perpage=" + str(n_pages) + "&access_token=" + g_strToken
        resp = requests.get(strlinkHotel).json()
        for hotel in resp["data"]: g_arr_hotel.append(hotel)

        str_delete_query = "DELETE from hotel WHERE hotel_id IN ("
        for hotel in g_arr_hotel:
            str_delete_query = str_delete_query + str(hotel["hotel_id"]) + ","
        str_delete_query = str_delete_query[:-1] + ")"
        cursor.execute(str_delete_query)

        str_insert_query = """INSERT INTO hotel (hotel_id, hotel_organization_id, hotel_language, hotel_name, hotel_date_modified, hotel_is_active, hotel_deleted, hotel_date_created, hotel_created_by, hotel_modified_by, hotel_network_id, last_update, hotel_energy_defaults_on, hotel_equipment_defaults_on, hotel_humidity_on, hotel_deployment_stage, hotel_screen_settings_on, hotel_default_size_large, hotel_message_degrees_show, hotel_message_screen_contrast, hotel_default_energy_profile, hotel_default_equipment_profile, hotel_presets_on, hotel_pms_integration_on, hotel_additional_network_on, hotel_remote_temperature_control_on, hotel_multi_factor_authentication_on, hotel_btu_average, hotel_kwh_rate, hotel_scheduler_on, hotel_ei_on, hotel_ei_on_date, hotel_vip_ctrl_on, hotel_thermostat_ctrl_on, hotel_ei_saving_screen) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        arr_insert_query = list()
        for hotel in g_arr_hotel:
            each_hotel = list()
            each_hotel.append(isNull(str(hotel["hotel_id"])))
            each_hotel.append(isNull(str(hotel["hotel_organization_id"])))
            each_hotel.append(isNull(str(hotel["hotel_language"])))
            each_hotel.append(isNull(str(hotel["hotel_name"])))
            each_hotel.append(isNull(str(hotel["hotel_date_modified"])))
            each_hotel.append(isNull(str(hotel["hotel_is_active"])))
            each_hotel.append(isNull(str(hotel["hotel_deleted"])))
            each_hotel.append(isNull(str(hotel["hotel_date_created"])))
            each_hotel.append(isNull(str(hotel["hotel_created_by"])))
            each_hotel.append(isNull(str(hotel["hotel_modified_by"])))
            each_hotel.append(isNull(str(hotel["hotel_network_id"])))
            each_hotel.append(isNull(str(hotel["last_update"])))
            each_hotel.append(isNull(str(hotel["hotel_energy_defaults_on"])))
            each_hotel.append(isNull(str(hotel["hotel_equipment_defaults_on"])))
            each_hotel.append(isNull(str(hotel["hotel_humidity_on"])))
            each_hotel.append(isNull(str(hotel["hotel_deployment_stage"])))
            each_hotel.append(isNull(str(hotel["hotel_screen_settings_on"])))
            each_hotel.append(isNull(str(hotel["hotel_default_size_large"])))
            each_hotel.append(isNull(str(hotel["hotel_message_degrees_show"])))
            each_hotel.append(isNull(str(hotel["hotel_message_screen_contrast"])))
            each_hotel.append(isNull(str(hotel["hotel_default_energy_profile"])))
            each_hotel.append(isNull(str(hotel["hotel_default_equipment_profile"])))
            each_hotel.append(isNull(str(hotel["hotel_presets_on"])))
            each_hotel.append(isNull(str(hotel["hotel_pms_integration_on"])))
            each_hotel.append(isNull(str(hotel["hotel_additional_network_on"])))
            each_hotel.append(isNull(str(hotel["hotel_remote_temperature_control_on"])))
            each_hotel.append(isNull(str(hotel["hotel_multi_factor_authentication_on"])))
            each_hotel.append(isNull(str(hotel["hotel_btu_average"])))
            each_hotel.append(isNull(str(hotel["hotel_kwh_rate"])))
            each_hotel.append(isNull(str(hotel["hotel_scheduler_on"])))
            each_hotel.append(isNull(str(hotel["hotel_ei_on"])))
            each_hotel.append(isNull(str(hotel["hotel_ei_on_date"])))
            each_hotel.append(isNull(str(hotel["hotel_vip_ctrl_on"])))
            each_hotel.append(isNull(str(hotel["hotel_thermostat_ctrl_on"])))
            each_hotel.append(isNull(str(hotel["hotel_ei_saving_screen"])))
            arr_insert_query.append(each_hotel)
            
        cursor.executemany(str_insert_query, arr_insert_query)
    except Exception as e:
        print ("exception : get_hotel_list")
        print (str(e))

def get_room_list(cursor):
    try:
        for hotel in g_arr_hotel:
            strFirstRoom = "https://api.thermostatsolutions.com/v1/rooms/?page=0&perpage=1&hotel_id=" + str(hotel['hotel_id'])
            n_pages = get_total_page_count(strFirstRoom)

            strlinkRoom = "https://api.thermostatsolutions.com/v1/rooms/?hotel_id=" + str(hotel['hotel_id']) + "&page=0&perpage=" + str(n_pages) + "&access_token=" + g_strToken
            resp = requests.get(strlinkRoom).json()
            print(n_pages)
            print(str(len(resp["data"])))
    except Exception as e:
        print ("exception : get_room_list")
        print (str(e))

def main():
    db_conn = MySQLdb.connect(
        host="verdant.c5rdujz93n3m.us-east-1.rds.amazonaws.com",
        database="verdant",
        user="admin",
        password="qweasdzxcasdqwe"
    )

    cursor = db_conn.cursor()

    get_hotel_list(cursor)
    get_room_list(cursor)

    db_conn.commit()
    db_conn.close()


if __name__ == '__main__':
    main()