import json
import requests
import MySQLdb

# Get hotel list
g_strToken = ""

def isNull(entry):
    if (entry == "None"): return "null"
    return entry.replace("’", "'")

def get_hotel_list(cursor):
    try:
        g_reqHotel = "https://api.thermostatsolutions.com/v1/hotels"

        arr_hotel = list()
        for i in range(100):
            strlinkHotel = g_reqHotel + "/?page=" + str(i+1) + "&perpage=1&access_token=" + g_strToken
            resp = requests.get(strlinkHotel).json()
            hotel_data = resp["data"]

            if (hotel_data == []): break
            for hotel in hotel_data:
                arr_hotel.append(hotel)

        str_delete_query = "DELETE from hotel WHERE hotel_id IN ("
        for hotel in arr_hotel:
            str_delete_query = str_delete_query + str(hotel["hotel_id"]) + ","
        str_delete_query = str_delete_query[:-1] + ")"
        cursor.execute(str_delete_query)

        str_insert_query = """INSERT INTO hotel (hotel_id, hotel_organization_id, hotel_language, hotel_name, hotel_date_modified, hotel_is_active, hotel_deleted, hotel_date_created, hotel_created_by, hotel_modified_by, hotel_network_id, last_update, hotel_energy_defaults_on, hotel_equipment_defaults_on, hotel_humidity_on, hotel_deployment_stage, hotel_screen_settings_on, hotel_default_size_large, hotel_message_degrees_show, hotel_message_screen_contrast, hotel_default_energy_profile, hotel_default_equipment_profile, hotel_presets_on, hotel_pms_integration_on, hotel_additional_network_on, hotel_remote_temperature_control_on, hotel_multi_factor_authentication_on, hotel_btu_average, hotel_kwh_rate, hotel_scheduler_on, hotel_ei_on, hotel_ei_on_date, hotel_vip_ctrl_on, hotel_thermostat_ctrl_on, hotel_ei_saving_screen) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        arr_insert_query = list()
        for hotel in arr_hotel:
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

def main():
    db_conn = MySQLdb.connect(
        host="",
        database="",
        user="",
        password=""
    )

    cursor = db_conn.cursor()

    get_hotel_list(cursor)

    db_conn.commit()
    db_conn.close()


if __name__ == '__main__':
    main()