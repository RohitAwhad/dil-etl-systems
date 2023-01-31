systems_health_system_cols_to_rename = {
    'OnekeyId': 'OneKeyId',
    'InOnekey': 'InOneKey',
    'AhaSysid': 'AhaSystemId',
    'MajInvOwned': 'MajorityInvestorOwned',
    'SysMultistate': 'IsMultistate',
    'HealthSysName': 'HealthSystemName',
    'HealthSysId': 'Hsi',
    'HealthSysCity': 'City',
    'HealthSysState': 'StateAbbreviation',
    'SysHqCityLon': 'Latitude',
    'SysHqCityLat': 'Longitude',
    'SysOwnershipCatHcris': 'OwnershipTypeName',
}

systems_hospital_cols_to_rename = {
    'CompendiumHospitalId': 'CompendiumId',
    'Ccn': 'CcnId',
    'HospitalStreet': 'Address',
    'HospitalCity': 'City',
    'HospitalCounty': 'CountyTypeName',
    'HospitalZip': 'ZipCode',
    'HospitalState': 'StateAbbreviation',
    'HospitalLat': 'Latitude',
    'HospitalLon': 'Longitude',
    'HealthSysName': 'HealthSystemName',
    'HealthSysId': 'Hsi',
}

systems_state_cols_to_rename = {
    'State': 'StateAbbreviation',
}
