import json
import requests
import MySQLdb
import time
import uuid
from datetime import datetime

# API token
g_strToken = "769b49ae6b2fd4e6678d55cafd74f0c872969b68ed984d295096ddab0bb018b6"

# Global variables
g_arr_hotel = list()
g_arr_room = list()
g_arr_report = list()

def isNull(entry):
    if (entry == "None"): return "null"
    return entry.replace("\u2019", "'")

def get_total_page_count(strlink):
    req_link = strlink + "&access_token=" + g_strToken
    resp = requests.get(req_link).json()
    last_page = resp['meta']['links']['last']
    last_page_num = last_page[last_page.find('&page=')+6:last_page.find('&perpage=')]
    return last_page_num

def standard_date_format(strdate):
    return strdate.replace("T", " ").replace(".000Z", "")

def get_hotel_list(cursor):
    try:
        n_pages = get_total_page_count("https://api.thermostatsolutions.com/v1/hotels/?page=0&perpage=1")

        strlinkHotel = "https://api.thermostatsolutions.com/v1/hotels/?page=0&perpage=" + str(n_pages) + "&access_token=" + g_strToken
        resp = requests.get(strlinkHotel).json()
        for hotel in resp["data"]:
            data = json.dumps(hotel).replace("\u2019", "'")
            g_arr_hotel.append(json.loads(data))

        str_delete_query = "DELETE from hotel"
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
        str_room_delete_query = "DELETE from room"
        str_alert_delete_query = "DELETE from roomalerts"
        cursor.execute(str_room_delete_query)
        cursor.execute(str_alert_delete_query)

        for hotel in g_arr_hotel:
            print(hotel["hotel_id"])
            strFirstRoom = "https://api.thermostatsolutions.com/v1/rooms/?page=0&perpage=1&hotel_id=" + str(hotel['hotel_id'])
            n_pages = get_total_page_count(strFirstRoom)

            strlinkRoom = "https://api.thermostatsolutions.com/v1/rooms/?hotel_id=" + str(hotel['hotel_id']) + "&page=0&perpage=" + str(n_pages) + "&access_token=" + g_strToken
            resp = requests.get(strlinkRoom).json()

            arr_room = list()
            for room in resp["data"]:
                g_arr_room.append(room)
                arr_room.append(room)

            str_room_insert_query = """INSERT INTO room (room_id, room_hotel_id, room_name, room_date_created, room_date_modified, room_deleted, room_created_by, room_modified_by, room_energy_profile_id, room_vip_mode, room_equipment_profile_id, room_pms_enabled, room_pms_profile_id, room_number, room_edi_termostat_location, room_edi_termostat_location_hex_b1, room_edi_termostat_location_hex_b2, room_floor_plan_id, room_floor_plan_pos_x, room_floor_plan_pos_y, room_floor_number, room_network_id, room_humidity_allowed, room_energy_report_running, room_cooler_counter, room_heater_counter, room_system_runtime_counter, room_system_runtime_counter_last_value, room_cooler_counter_last_value, room_heater_counter_last_value, room_occupancy_flag, room_hw, room_sw, svn_pic, svn_radio, active_sensor, roomp_termostat_id, roomp_occupancy, roomp_temperature, roomp_thermostat, roomp_heat_setpoint, roomp_cool_setpoint, roomp_auto_setpoint, roomp_system_status_1, roomp_system_status_2, roomp_recovery_time, roomp_max_setpoint_heat, roomp_min_setpoint_cool) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            arr_room_insert_query = list()
            str_alert_insert_query = """INSERT INTO roomalerts (id, alert_type, cms_room_id) VALUES (%s, %s, %s)"""
            arr_alert_insert_query = list()
            for room in arr_room:
                each_room = list()
                each_room.append(isNull(str(room["room_id"])))
                each_room.append(isNull(str(room["room_hotel_id"])))
                each_room.append(isNull(str(room["room_name"])))
                each_room.append(isNull(str(room["room_date_created"])))
                each_room.append(isNull(str(room["room_date_modified"])))
                each_room.append(isNull(str(room["room_deleted"])))
                each_room.append(isNull(str(room["room_created_by"])))
                each_room.append(isNull(str(room["room_modified_by"])))
                each_room.append(isNull(str(room["room_energy_profile_id"])))
                each_room.append(isNull(str(room["room_vip_mode"])))
                each_room.append(isNull(str(room["room_equipment_profile_id"])))
                each_room.append(isNull(str(room["room_pms_enabled"])))
                each_room.append(isNull(str(room["room_pms_profile_id"])))
                each_room.append(isNull(str(room["room_number"])))
                each_room.append(isNull(str(room["room_edi_termostat_location"])))
                each_room.append(isNull(str(room["room_edi_termostat_location_hex_b1"])))
                each_room.append(isNull(str(room["room_edi_termostat_location_hex_b2"])))
                each_room.append(isNull(str(room["room_floor_plan_id"])))
                each_room.append(isNull(str(room["room_floor_plan_pos_x"])))
                each_room.append(isNull(str(room["room_floor_plan_pos_y"])))
                each_room.append(isNull(str(room["room_floor_number"])))
                each_room.append(isNull(str(room["room_network_id"])))
                each_room.append(isNull(str(room["room_humidity_allowed"])))
                each_room.append(isNull(str(room["room_energy_report_running"])))
                each_room.append(isNull(str(room["room_cooler_counter"])))
                each_room.append(isNull(str(room["room_heater_counter"])))
                each_room.append(isNull(str(room["room_system_runtime_counter"])))
                each_room.append(isNull(str(room["room_system_runtime_counter_last_value"])))
                each_room.append(isNull(str(room["room_cooler_counter_last_value"])))
                each_room.append(isNull(str(room["room_heater_counter_last_value"])))
                each_room.append(isNull(str(room["room_occupancy_flag"])))
                each_room.append(isNull(str(room["room_hw"])))
                each_room.append(isNull(str(room["room_sw"])))
                each_room.append(isNull(str(room["svn_pic"])))
                each_room.append(isNull(str(room["svn_radio"])))
                each_room.append(isNull(str(room["active_sensor"])))
                each_room.append(isNull(str(room["room_params"]["termostat_id"])) if room["room_params"].get("termostat_id") != None else "null")
                each_room.append(isNull(str(room["room_params"]["occupancy"])) if room["room_params"].get("occupancy") != None else "null")
                each_room.append(isNull(str(room["room_params"]["temperature"])) if room["room_params"].get("temperature") != None else "null")
                each_room.append(isNull(str(room["room_params"]["thermostat"])) if room["room_params"].get("thermostat") != None else "null")
                each_room.append(isNull(str(room["room_params"]["heat_setpoint"])) if room["room_params"].get("heat_setpoint") != None else "null")
                each_room.append(isNull(str(room["room_params"]["cool_setpoint"])) if room["room_params"].get("cool_setpoint") != None else "null")
                each_room.append(isNull(str(room["room_params"]["auto_setpoint"])) if room["room_params"].get("auto_setpoint") != None else "null")
                each_room.append(isNull(str(room["room_params"]["system_status_1"])) if room["room_params"].get("system_status_1") != None else "null")
                each_room.append(isNull(str(room["room_params"]["system_status_2"])) if room["room_params"].get("system_status_2") != None else "null")
                each_room.append(isNull(str(room["room_params"]["recovery_time"])) if room["room_params"].get("recovery_time") != None else "null")
                each_room.append(isNull(str(room["room_params"]["max_setpoint_heat"])) if room["room_params"].get("max_setpoint_heat") != None else "null")
                each_room.append(isNull(str(room["room_params"]["min_setpoint_cool"])) if room["room_params"].get("min_setpoint_cool") != None else "null")
                arr_room_insert_query.append(each_room)

                for alert in room['room_alerts']:
                    each_alert = list()
                    each_alert.append(alert['id'])
                    each_alert.append(isNull(str(alert["alert_type"])))
                    each_alert.append(isNull(str(alert["cms_room_id"])))
                    arr_alert_insert_query.append(each_alert)

            cursor.executemany(str_room_insert_query, arr_room_insert_query)
            cursor.executemany(str_alert_insert_query, arr_alert_insert_query)
    except Exception as e:
        print ("exception : get_room_list")
        print (str(e))

def get_report_list(cursor):
    try:
        str_report_delete_query = "DELETE from reports"
        cursor.execute(str_report_delete_query)

        str_report_insert_query = """INSERT INTO reports (room_id, from_time, to_time, comp_runtime, heater_runtime, thermostat_id) VALUES (%s, %s, %s, %s, %s, %s)"""
        arr_report_insert_query = list()
        for room in g_arr_room:
            str_room_id = room['room_id']
            str_from = room['room_date_created']
            str_to = datetime.today().strftime('%Y-%m-%dT%H:%M:%S.000Z')
            str_request_url = """https://api.thermostatsolutions.com/v1/reports/runtime/hotel/room/%s?from=%s&to=%s&access_token=%s""" % (str_room_id, str_from, str_to, g_strToken)
            reports = requests.get(str_request_url).json()

            for report in reports:
                each_report = list()
                each_report.append(isNull(str(str_room_id)))
                each_report.append(isNull(str(str_from)))
                each_report.append(isNull(str(str_to)))
                each_report.append(isNull(str(report["comp_runtime"])))
                each_report.append(isNull(str(report["heater_runtime"])))
                each_report.append(isNull(str(report["room_id"])))
                arr_report_insert_query.append(each_report)
                g_arr_report.append(each_report)

        cursor.executemany(str_report_insert_query, arr_report_insert_query)
    except Exception as e:
        print ("exception : get_report_data")
        print (str(e))

def get_unoccupied_runtime(cursor):
    try:
        str_ocr_delete_query = """DELETE from unoccupied_runtime"""
        cursor.execute(str_ocr_delete_query)

        for index, report in enumerate(g_arr_report):
            print(str(index) + ": get_unoccupied_runtime")
            time.sleep(1)
            str_request_url = """https://api.thermostatsolutions.com/v1/thermostats/%s/history?from_date=%s&to_date=%s&access_token=%s""" % (report[5], report[1], report[2], g_strToken)
            u_a = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.82 Safari/537.36"

            recall = 0
            err_cnt = 0
            unoccupied_runtime = requests.get(str_request_url, headers={"USER-AGENT": u_a})
            recall = unoccupied_runtime.status_code

            while (recall != 200):                
                time.sleep(1)
                unoccupied_runtime = requests.get(str_request_url, headers={"USER-AGENT": u_a})
                recall = unoccupied_runtime.status_code
                err_cnt = err_cnt + 1
                print (str(recall))
                if (err_cnt > 20): break                
            
            if (err_cnt > 20): continue

            unoccupied_runtime = unoccupied_runtime.json()

            if (len(unoccupied_runtime["data"]) == 0): continue

            str_ocr_insert_query = """INSERT INTO unoccupied_runtime (s2, c, pt, f, hsp, h, occaux, ttl, occ, dt, t, ob, thermostat_id, hum, csp, sp, s1) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            arr_ocr_insert_query = list()

            for data in unoccupied_runtime["data"]:                
                arr_unoccupired_runtime = list()
                arr_unoccupired_runtime.append(isNull(str(data["s2"])))
                arr_unoccupired_runtime.append(isNull(str(data["c"])))
                arr_unoccupired_runtime.append(isNull(str(data["pt"])))
                arr_unoccupired_runtime.append(isNull(str(data["f"])))
                arr_unoccupired_runtime.append(isNull(str(data["hsp"])))
                arr_unoccupired_runtime.append(isNull(str(data["h"])))
                arr_unoccupired_runtime.append(isNull(str(data["occaux"])))
                arr_unoccupired_runtime.append(isNull(str(data["ttl"])))
                arr_unoccupired_runtime.append(isNull(str(data["occ"])))
                arr_unoccupired_runtime.append(isNull(str(data["dt"])))
                arr_unoccupired_runtime.append(isNull(str(data["t"])))
                arr_unoccupired_runtime.append(isNull(str(data["ob"])))
                arr_unoccupired_runtime.append(isNull(str(data["id"])))
                arr_unoccupired_runtime.append(isNull(str(data["hum"])))
                arr_unoccupired_runtime.append(isNull(str(data["csp"])))
                arr_unoccupired_runtime.append(isNull(str(data["sp"])))
                arr_unoccupired_runtime.append(isNull(str(data["s1"])))
                arr_ocr_insert_query.append(arr_unoccupired_runtime)

            cursor.executemany(str_ocr_insert_query, arr_ocr_insert_query)
    except Exception as e:
        print ("exception : get unoccupired data")
        print (str(e))

def get_occ(cursor):
    try:
        str_occ_delete_query = """DELETE from occhistory"""
        cursor.execute(str_occ_delete_query)

        for index, report in enumerate(g_arr_report):
            print(str(index) + ": get_occ")
            time.sleep(1)
            str_request_url = """https://api.thermostatsolutions.com/v1/thermostats/%s/occ-history?from_date=%s&to_date=%s&access_token=%s""" % (report[5], report[1], report[2], g_strToken)
            
            u_a = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.82 Safari/537.36"

            recall = 0
            err_cnt = 0
            occ_list = requests.get(str_request_url, headers={"USER-AGENT": u_a})
            recall = occ_list.status_code

            while (recall != 200):                
                time.sleep(1)
                occ_list = requests.get(str_request_url, headers={"USER-AGENT": u_a})
                recall = occ_list.status_code
                err_cnt = err_cnt + 1
                print (str(recall))
                if (err_cnt > 20): break                
            
            if (err_cnt > 20): continue

            occ_list = occ_list.json()

            if (len(occ_list["data"]) == 0): continue

            str_occ_insert_query = """INSERT INTO occhistory (occ_status, start_time, finish_time, thermostat_id) VALUES (%s, %s, %s, %s)"""
            arr_occ_insert_query = list()
            for data in occ_list["data"]:
                arr_occ = list()
                arr_occ.append(isNull(str(data["occ"])))
                arr_occ.append(isNull(str(data["start"])))
                arr_occ.append(isNull(str(data["finish"])))
                arr_occ.append(isNull(str(report[5])))
                arr_occ_insert_query.append(arr_occ)

            cursor.executemany(str_occ_insert_query, arr_occ_insert_query)
    except Exception as e:
        print ("exception : get occ data")
        print (str(e))
    
def set_bi_data(cursor):
    try:
        str_delete_forecast = """DELETE FROM forecast"""
        cursor.execute(str_delete_forecast)
        str_insert_forecast = """
            INSERT INTO forecast (hotel_id, hotel_name, room_id, thermostat_id, from_time, to_time, total_days, total_mins, occ_mins, unocc_mins, unocc_runtime, occ_runtime, saved_time)
            SELECT fst.*, snd.ucr, thr.ucr, (fst.unocc_mins - snd.ucr) AS saved_time
            FROM
            (SELECT hotel.hotel_id, hotel.hotel_name, room.room_id, reports.thermostat_id, reports.from_time, reports.to_time,
                TIMESTAMPDIFF(HOUR, reports.from_time, NOW()) / 24 AS total_days,
                TIMESTAMPDIFF(HOUR, reports.from_time, NOW()) * 60 AS total_mins,
                SUM(IF(occhistory.occ_status != 0, TIMESTAMPDIFF(SECOND, occhistory.start_time, occhistory.finish_time) / 60, 0)) AS occ_mins,
                SUM(IF(occhistory.occ_status = 0, TIMESTAMPDIFF(SECOND, occhistory.start_time, occhistory.finish_time) / 60, 0)) AS unocc_mins
                FROM reports
                LEFT JOIN room
                ON room.room_id = reports.room_id
                LEFT JOIN hotel
                ON hotel.hotel_id = room.room_hotel_id
                LEFT JOIN occhistory
                ON occhistory.thermostat_id = reports.thermostat_id
                GROUP BY reports.thermostat_id) AS fst,
                (SELECT ur.c, ur.h, o.occ_status, ur.dt, o.finish_time, o.thermostat_id, SUM(TIMESTAMPDIFF(SECOND, ur.dt, o.finish_time) / 60) AS ucr
                FROM unoccupied_runtime ur, occhistory o
                WHERE ur.occ = 0 AND (ur.c > 0 OR ur.h > 0) AND ur.dt = o.start_time AND ur.thermostat_id = o.thermostat_id AND o.occ_status = 0
                GROUP BY ur.thermostat_id) AS snd,
                (SELECT ur.c, ur.h, o.occ_status, ur.dt, o.finish_time, o.thermostat_id, SUM(TIMESTAMPDIFF(SECOND, ur.dt, o.finish_time) / 60) AS ucr
                FROM unoccupied_runtime ur, occhistory o
                WHERE ur.occ > 0 AND (ur.c > 0 OR ur.h > 0) AND ur.dt = o.start_time AND ur.thermostat_id = o.thermostat_id AND o.occ_status > 0
                GROUP BY ur.thermostat_id) AS thr
                WHERE fst.thermostat_id = snd.thermostat_id AND snd.thermostat_id = thr.thermostat_id"""
        cursor.execute(str_insert_forecast)
    except Exception as e:
        print ("exception : set bit data")
        print (str(e))
def main():
    start = time.time()
    db_conn = MySQLdb.connect(
        host="verdantdb.mysql.database.azure.com",
        database="verdant",
        user="admin_verdant@verdantdb",
        password="qweasdzxcasdqwe"
    )

    cursor = db_conn.cursor()

    get_hotel_list(cursor)
    get_room_list(cursor)
    get_report_list(cursor)
    get_unoccupied_runtime(cursor)
    get_occ(cursor)
    #set_bi_data(cursor)

    db_conn.commit()
    db_conn.close()
    end = time.time()
    print (end-start)
    print (str(datetime.now()))


if __name__ == '__main__':
    main()