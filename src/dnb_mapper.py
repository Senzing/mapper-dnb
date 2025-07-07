#! /usr/bin/env python3

import argparse
import csv
import glob
import json
import os
import random
import signal
import sys
import time
from datetime import datetime, timedelta


# ----------------------------------------
def processFile(inputFileName):
    updateStat("INPUT", "FILE_COUNT")
    global shutDown, outputFileHandle, outputFileName

    # --set up a reader
    schemaData = dnbFormats["schemas"][dnbFormats["mappings"][dnbFormat]["inputSchema"]]

    try:
        if "encoding" in schemaData:
            inputFileHandle = open(inputFileName, "r", encoding=schemaData["encoding"])
        else:
            inputFileHandle = open(inputFileName, "r")
    except IOError as err:
        print("")
        print(err)
        print("")
        return 1

    if schemaData["fileType"].upper() == "JSON":
        inputFileReader = inputFileHandle
    else:

        # --set csv dialect
        if schemaData["fileType"].upper() == "CHECK":
            sniffer = csv.Sniffer().sniff(inputFileHandle.readline(), delimiters="|,\t")
            inputFileHandle.seek(0)
            delimiter = sniffer.delimiter
        elif schemaData["fileType"].upper() == "TAB":
            delimiter = "\t"
        elif schemaData["fileType"].upper() == "PIPE":
            delimiter = "|"
        elif schemaData["fileType"].upper() == "CSV":
            delimiter = ","
        else:
            delimiter = schemaData["delimiter"] if "delimiter" in schemaData else None
        if not delimiter:
            print("")
            print("File type %s not supported")
            print("")
            return 1
        quotechar = schemaData["quotechar"] if "quotechar" in schemaData else None

        try:
            if quotechar:
                inputFileReader = csv.reader(
                    inputFileHandle, delimiter=delimiter, quotechar=quotechar
                )
            else:
                inputFileReader = csv.reader(inputFileHandle, delimiter=delimiter)
        except csv.Error as err:
            print("")
            print(err)
            print("")
            return 1

        # --see if first row contains column header
        headerCount = 0
        firstRowValues = next(inputFileReader)
        for columnValue in firstRowValues:
            if columnValue.upper() in schemaData["columns"]:
                headerCount += 1
        percentHeader = (headerCount / len(schemaData["columns"])) * 100
        if percentHeader >= 90:
            if percentHeader != 100:
                print(
                    "warning: first row has %s of %s of expected column headers!"
                    % (headerCount, len(schemaData["columns"]))
                )
            schemaData["columns"] = [x.upper() for x in firstRowValues]
        else:
            print(
                "warning: first does not contain the column header! (%s)"
                % percentHeader
            )
            # for i in range(len(schemaData['columns'])):

            inputFileHandle.seek(0)

    # --open an output file if output is a directory
    if not outputIsFile:
        outputFileName = outputFilePath + os.path.basename(inputFileName) + ".json"
        try:
            outputFileHandle = open(outputFileName, "w", encoding="utf-8")
        except IOError as err:
            print("")
            print("Could not open output file %s for writing" % outputFileName)
            print(" %s" % err)
            print("")
            sys.exit(1)

    # --to de-dupe records if needed
    ubo_company_cache = {}
    ubo_depth_cache = {}
    # {
    #  "SUBJECT_ID": {
    #    "1": [
    #      "id1",
    #      "id2"
    #    ]
    #  }
    # }
    fileStartTime = time.time()
    batchStartTime = time.time()
    badCnt = 0
    rowCnt = 0
    for row in inputFileReader:
        updateStat("INPUT", "ROW_COUNT")
        rowCnt += 1
        rowData = None

        # --validate json
        if schemaData["fileType"].upper() == "JSON":
            try:
                rowData = json.loads(row)
            except:
                print("Invalid json in row %s" % rowCnt)

        # --validate csv
        elif len(row) != len(schemaData["columns"]):
            print(
                'Column mismatch in row %s: expected %s columns, got %s ... "%s"'
                % (
                    rowCnt,
                    len(schemaData["columns"]),
                    len(row),
                    delimiter.join(row)[0:50],
                )
            )
        elif schemaData["columns"][0].upper() + "|" + schemaData["columns"][
            1
        ].upper() == (str(row[0]).upper() if row[0] else "") + "|" + (
            str(row[1]).upper() if row[1] else ""
        ):
            print("Column header detected in row %s" % rowCnt)
            continue
        else:
            rowData = dict(zip(schemaData["columns"], row))

        # --bad row processing
        if not rowData:
            badCnt += 1
            if badCnt == 10 and rowCnt == 10:
                print("")
                print("Shutting down, too many errors")
                print("")
                shutDown = True  # --bad csv header
                break
            else:
                continue

        # --perform the mapping
        if dnbFormat == "UBO":
            jsonList = format_UBO(rowData)
        elif dnbFormat == "GCA":
            jsonList = format_GCA(rowData)
        elif dnbFormat == "CMPCVF":
            jsonList = format_CMPCVF(rowData)
        elif dnbFormat == "UBO_ALONE":
            jsonList1, ubo_company_cache = format_UBO_SUBJECT(
                rowData, ubo_company_cache
            )
            jsonList2, ubo_depth_cache = format_UBO2(rowData, ubo_depth_cache)
            jsonList = jsonList1 + jsonList2
        else:
            print("")
            print("No conversions for format code %s" % dnbFormat)
            print("")
            shutDown = True
            break

        # --process each json record returned
        for jsonData in jsonList:

            # --write it to file
            msg = json.dumps(jsonData)
            try:
                outputFileHandle.write(msg + "\n")
            except IOError as err:
                print("")
                print("Could not write to %s" % outputFileName)
                print(" %s" % err)
                print("")
                shutDown = True
                break

        if rowCnt % progressInterval == 0:
            now = datetime.now().strftime("%I:%M%p").lower()
            elapsedMins = round((time.time() - procStartTime) / 60, 1)
            eps = int(
                float(progressInterval)
                / (
                    float(
                        time.time() - batchStartTime
                        if time.time() - batchStartTime != 0
                        else 1
                    )
                )
            )
            batchStartTime = time.time()
            print(" %s records processed at %s, %s per second" % (rowCnt, now, eps))

        if shutDown:
            break

    if not shutDown:
        now = datetime.now().strftime("%I:%M%p").lower()
        elapsedMins = round((time.time() - fileStartTime) / 60, 1)
        eps = int(
            float(rowCnt)
            / (
                float(
                    time.time() - fileStartTime
                    if time.time() - fileStartTime != 0
                    else 1
                )
            )
        )
        print(
            " %s records processed at %s, %s per second, complete!" % (rowCnt, now, eps)
        )

    # --close all inputs and outputs
    # --open an output file if output is a directory
    if not outputIsFile:
        outputFileHandle.close()
    inputFileHandle.close()

    return shutDown


# ----------------------------------------
def format_CMPCVF(rowData):

    jsonList = []

    # --data corrections / updates
    rowData = rowData["organization"]
    recordType = "ORGANIZATION"
    statCategory = "DNB_COMPANY"

    # --json header
    jsonData = {}
    jsonData["DATA_SOURCE"] = "DNB-COMPANY"
    jsonData["RECORD_ID"] = rowData["duns"]
    jsonData["RECORD_TYPE"] = recordType

    jsonData["DUNS_NUMBER"] = rowData["duns"]
    updateStat(statCategory, "DUNS_NUMBER", rowData["duns"])
    thisDuns = rowData["duns"]

    # --map the names
    bestName = ""
    nameFields = {}
    nameFields["primaryName"] = "PRIMARY"
    nameFields["registeredName"] = "REGISTERED"
    for nameField in nameFields.keys():
        if nameField in rowData and rowData[nameField]:
            nameType = nameFields[nameField]
            jsonData[nameType + "_NAME_ORG"] = rowData[nameField]
            updateStat(statCategory, "NAME_ORG_" + nameType, rowData[nameField])
            if nameType == "PRIMARY":
                bestName = rowData[nameField]

    thisList = []
    for record in rowData.get("formerPrimaryNames", list()):
        if record.get("name"):
            thisList.append({"NAME_TYPE": "FORMER", "NAME_ORG": record["name"]})
            updateStat(statCategory, "NAME_ORG_" + "FORMER", record["name"])

    for record in rowData.get("formerRegisteredNames", list()):
        if record.get("name"):
            thisList.append({"NAME_TYPE": "FORMER", "NAME_ORG": record["name"]})
            updateStat(statCategory, "NAME_ORG_" + "FORMER", record["name"])

    for record in rowData.get("tradeStyleNames", list()):
        if record.get("name"):
            thisList.append({"NAME_TYPE": "TRADE", "NAME_ORG": record["name"]})
            updateStat(statCategory, "NAME_ORG_" + "TRADE", record["name"])

    if thisList:
        jsonData["ADDITIONAL_NAMES"] = thisList

    # --map the addresses
    addrFields = {}
    addrFields["primaryAddress"] = "PRIMARY"
    addrFields["registeredAddress"] = "REGISTERED"
    addrFields["mailingAddress"] = "MAILING"
    addrFields["formerRegisteredAddress"] = "FORMER"
    for addressField in addrFields.keys():
        if addressField in rowData and rowData[addressField]:
            addrType = addrFields[addressField]
            fullAddress, jsonAddr = mapJsonAddr(
                rowData[addressField], addrType, thisDuns
            )
            if fullAddress:
                jsonData.update(jsonAddr)
                updateStat(statCategory, "ADDRESS_" + addrType, fullAddress)

    # --phone numbers
    thisList = []
    for record in rowData["telephone"] if "telephone" in rowData else []:
        if record["telephoneNumber"]:
            phoneNumber = (
                "+" + record["isdCode"] + " "
                if "isdCode" in record and record["isdCode"]
                else ""
            )
            phoneNumber += record["telephoneNumber"]
            thisList.append({"PHONE_NUMBER": phoneNumber})
            updateStat(statCategory, "PHONE_NUMBER", phoneNumber)
    if thisList:
        jsonData["TELEPHONES"] = thisList

    # --website addresses
    thisList = []
    for record in rowData["websiteAddress"] if "websiteAddress" in rowData else []:
        thisList.append({"WEBSITE_ADDRESS": record["url"]})
        updateStat(statCategory, "WEBSITE_ADDRESS", record["url"])
    if thisList:
        jsonData["WEBSITES"] = thisList

    # --email addresses
    thisList = []
    for record in rowData["email"] if "email" in rowData else []:
        thisList.append({"EMAIL_ADDRESS": record["address"]})
        updateStat(statCategory, "EMAIL_ADDRESS", record["address"])
    if thisList:
        jsonData["EMAILS"] = thisList

    # --other ID numbers
    thisList = []
    for record in (
        rowData["registrationNumbers"] if "registrationNumbers" in rowData else []
    ):
        if record["typeDescription"] == "Federal Taxpayer Identification Number (US)":
            thisList.append({"TAX_ID_NUMBER": record["registrationNumber"]})
            thisList.append({"TAX_ID_COUNTRY": "US"})
            updateStat(
                statCategory,
                "TAX_ID:" + record["typeDescription"],
                record["registrationNumber"],
            )
        else:
            thisList.append({"OTHER_ID_NUMBER": record["registrationNumber"]})
            if record.get("typeDescription"):
                thisList.append({"OTHER_ID_TYPE": record["typeDescription"]})
                updateStat(
                    statCategory,
                    "OTHER_ID:" + record["typeDescription"],
                    record["registrationNumber"],
                )
            else:
                updateStat(statCategory, "OTHER_ID:" + "", record["registrationNumber"])

    if thisList:
        jsonData["OTHER_IDS"] = thisList

    # --industry codes
    thisList = []
    for record in rowData.get("industryCodes", list()):
        # description can be null
        codeData = f'{record.get("code", str())} {"(" + record["description"] + ")" if record.get("description") else str()}'
        # typeDescription can be null
        if record.get("typeDescription"):
            updateStat(
                statCategory, "INDUSTRY_CODE:" + record["typeDescription"], codeData
            )
            thisList.append(
                {
                    "INDUSTRY_CODE_VALUE": codeData,
                    "INDUSTRY_CODE_TYPE": record["typeDescription"],
                }
            )
        else:
            updateStat(statCategory, "INDUSTRY_CODE:" + "", codeData)
            thisList.append({"INDUSTRY_CODE_VALUE": codeData})
    if thisList:
        jsonData["INDUSTRY_CODES"] = thisList

    # --non resolving attributes
    if (
        "dunsControlStatus" in rowData
        and "operatingStatus" in rowData["dunsControlStatus"]
        and "description" in rowData["dunsControlStatus"]["operatingStatus"]
        and rowData["dunsControlStatus"]["operatingStatus"]["description"]
    ):
        jsonData["OPERATING_STATUS"] = rowData["dunsControlStatus"]["operatingStatus"][
            "description"
        ]
        updateStat(statCategory, "OPERATING_STATUS", jsonData["OPERATING_STATUS"])
    if (
        "businessEntityType" in rowData
        and "description" in rowData["businessEntityType"]
        and rowData["businessEntityType"]["description"]
    ):
        jsonData["BUSINESS_TYPE"] = rowData["businessEntityType"]["description"]
        updateStat(statCategory, "OPERATING_STATUS", jsonData["BUSINESS_TYPE"])
    if (
        "legalForm" in rowData
        and rowData["legalForm"].get("description")
        and rowData["businessEntityType"].get("description")
    ):
        jsonData["LEGAL_FORM"] = rowData["businessEntityType"].get("description")

        updateStat(statCategory, "LEGAL_FORM", jsonData["LEGAL_FORM"])

    if "incorporatedDate" in rowData and rowData["incorporatedDate"]:
        jsonData["INCORPORATED_DATE"] = rowData["incorporatedDate"]
        updateStat(statCategory, "INCORPORATED_DATE", jsonData["INCORPORATED_DATE"])

    if "startDate" in rowData and rowData["startDate"]:
        jsonData["START_DATE"] = rowData["startDate"]
        updateStat(statCategory, "START_DATE", jsonData["START_DATE"])

    # --so others can link to this entity
    jsonData["REL_ANCHOR_DOMAIN"] = "DUNS"
    jsonData["REL_ANCHOR_KEY"] = thisDuns

    # --add parent entities and their relationships
    if "corporateLinkage" in rowData:
        relationships = []
        parentTags = {}
        parentTags["globalUltimate"] = "Global Parent"
        parentTags["domesticUltimate"] = "Ultimate Parent"
        parentTags["parent"] = "Direct Parent"
        parentTags["headquarter"] = "Headquarters"
        for parentTag in parentTags.keys():
            if (
                parentTag in rowData["corporateLinkage"]
                and rowData["corporateLinkage"][parentTag]
                and rowData["corporateLinkage"][parentTag]["duns"] != thisDuns
            ):
                rowData1 = rowData["corporateLinkage"][parentTag]
                jsonData1 = {}
                jsonData1["DATA_SOURCE"] = "DNB-PARENT"
                jsonData1["RECORD_TYPE"] = "ORGANIZATION"
                jsonData1["RECORD_ID"] = rowData1["duns"]
                updateStat("PARENT", parentTag)
                jsonData1["DUNS_NUMBER"] = rowData1["duns"]
                jsonData1["REL_ANCHOR_DOMAIN"] = "DUNS"
                jsonData1["REL_ANCHOR_KEY"] = rowData1["duns"]
                if "primaryName" in rowData1 and rowData1["primaryName"]:
                    jsonData1["NAME_ORG"] = rowData1["primaryName"]
                    updateStat("PARENT", parentTag, rowData1["primaryName"])
                if "primaryAddress" in rowData1 and rowData1["primaryAddress"]:
                    addrType = "PRIMARY"
                    fullAddress, jsonAddr = mapJsonAddr(
                        rowData1["primaryAddress"], addrType
                    )
                    if fullAddress:
                        jsonData1.update(jsonAddr)
                        updateStat("PARENT", "ADDRESS+" + addrType, fullAddress)
                if jsonData1 not in jsonList:
                    jsonList.append(jsonData1)

                relationship = {}
                relationship["REL_POINTER_DOMAIN"] = "DUNS"
                relationship["REL_POINTER_KEY"] = rowData1["duns"]
                relationship["REL_POINTER_ROLE"] = parentTags[parentTag]
                relationships.append(relationship)
        if relationships:
            jsonData["RELATIONSHIPS"] = relationships

    # --current and most senior executives
    principleList = []
    if "mostSeniorPrincipals" in rowData and rowData["mostSeniorPrincipals"]:
        for rowData1 in rowData["mostSeniorPrincipals"]:
            rowData1["principleType"] = "Senior Principle"
            principleList.append(rowData1)
    if "currentPrincipals" in rowData and rowData["currentPrincipals"]:
        for rowData1 in rowData["currentPrincipals"]:
            rowData1["principleType"] = "Current Principle"
            try:
                principleList.append(rowData1)
            except:
                print(rowData1)

    principleCnt = 0
    for rowData1 in principleList:
        fullName = rowData1.get("fullName")
        familyName = rowData1.get("familyName")
        if not fullName and not familyName:
            updateStat(statCategory, "NO_NAME_SKIP", json.dumps(rowData1))
            continue
        principleCnt += 1

        recordType1 = "PERSON"
        statCategory = "PRINCIPLE"
        updateStat(
            statCategory,
            "subjectType",
            rowData1["subjectType"] if "subjectType" in rowData1 else "missing",
        )

        jsonData1 = {}
        jsonData1["DATA_SOURCE"] = "DNB-PRINCIPLE"
        jsonData1["RECORD_ID"] = "%s-PR-%s" % (thisDuns, principleCnt)
        jsonData1["RECORD_TYPE"] = recordType1

        if fullName:
            fullName = rowData1["fullName"].strip()
            jsonData1["PRIMARY_NAME_FULL"] = fullName
            updateStat(statCategory, "FULL_NAME", fullName)
        else:
            fullName = ""
            if "namePrefix" in rowData1 and rowData1["namePrefix"]:
                fullName += " " + rowData1["namePrefix"]
                jsonData1["PRIMARY_NAME_PREFIX"] = rowData1["namePrefix"]
            if "givenName" in rowData1 and rowData1["givenName"]:
                fullName += " " + rowData1["givenName"]
                jsonData1["PRIMARY_NAME_FIRST"] = rowData1["givenName"]
            if "middleName" in rowData1 and rowData1["middleName"]:
                fullName += " " + rowData1["middleName"]
                jsonData1["PRIMARY_NAME_MIDDLE"] = rowData1["middleName"]
            if "familyName" in rowData1 and rowData1["familyName"]:
                fullName += " " + rowData1["familyName"]
                jsonData1["PRIMARY_NAME_LAST"] = rowData1["familyName"]
            if "nameSuffix" in rowData1 and rowData1["nameSuffix"]:
                fullName += " " + rowData1["nameSuffix"]
                jsonData1["PRIMARY_NAME_SUFFIX"] = rowData1["nameSuffix"]
            if fullName:
                updateStat(statCategory, "PARSED_NAME", fullName.strip())

        # --map the address
        if "primaryAddress" in rowData1 and rowData1["primaryAddress"]:
            addrType = "PRIMARY"
            fullAddress, jsonAddr = mapJsonAddr(rowData1["primaryAddress"], addrType)
            if fullAddress:
                jsonData1.update(jsonAddr)
                updateStat(statCategory, "ADDRESS+" + addrType, fullAddress)

        if "birthDate" in rowData1 and rowData1["birthDate"]:
            jsonData1["DATE_OF_BIRTH"] = rowData1["birthDate"]
            updateStat(statCategory, "DOB", rowData1["birthDate"])
        if (
            "gender" in rowData1
            and "description" in rowData1["gender"]
            and rowData1["gender"]["description"]
        ):
            jsonData1["GENDER"] = rowData1["gender"]["description"]
            updateStat(statCategory, "GENDER", rowData1["gender"]["description"])
        if (
            "nationality" in rowData1
            and "isoAlpha2Code" in rowData1["nationality"]
            and rowData1["nationality"]["isoAlpha2Code"]
        ):
            jsonData1["NATIONALITY"] = rowData1["nationality"]["isoAlpha2Code"]
            updateStat(
                statCategory, "NATIONALITY", rowData1["nationality"]["isoAlpha2Code"]
            )

        jobTitleList = []
        if "jobTitles" in rowData1 and rowData1["jobTitles"]:
            for titleData in rowData1["jobTitles"]:
                jobTitleList.append(titleData["title"])
                updateStat(statCategory, "JOB_TITLE", titleData["title"])
            jsonData1["JOB_TITLE"] = ",".join(jobTitleList)

        # --relate them to the company and use their group association for matching
        jsonData1["REL_POINTER_DOMAIN"] = "DUNS"
        jsonData1["REL_POINTER_KEY"] = thisDuns
        jsonData1["REL_POINTER_ROLE"] = rowData1["principleType"]

        jsonData1["GROUP_ASSN_ID_TYPE"] = "DUNS"
        jsonData1["GROUP_ASSN_ID_NUMBER"] = thisDuns
        updateStat(statCategory, "GROUP_ASSN_ID", thisDuns)
        if bestName:
            jsonData1["GROUP_ASSOCIATION_ORG_NAME"] = bestName
            updateStat(statCategory, "GROUP_ASSOCIATION_NAME", bestName)

        # --current and most senior principles overlap
        if jsonData1 not in jsonList:
            jsonList.append(jsonData1)

    # --add the primary entity
    jsonList.append(jsonData)

    return jsonList


# ----------------------------------------
def mapJsonAddr(addrData, usageType, recordID=None):
    checkit = False
    jsonAddr = {}
    addrType = usageType + "_"
    fullAddress = ""
    if (
        "streetAddress" in addrData
        and "line1" in addrData["streetAddress"]
        and addrData["streetAddress"]["line1"]
    ):
        addrValue = addrData["streetAddress"]["line1"]
        jsonAddr[addrType + "ADDR_LINE1"] = addrValue
        fullAddress = addrValue
        if "line2" in addrData["streetAddress"] and addrData["streetAddress"]["line2"]:
            addrValue = addrData["streetAddress"]["line2"]
            jsonAddr[addrType + "ADDR_LINE2"] = addrValue
            fullAddress = (fullAddress + " " + addrValue).strip()
        if "line3" in addrData["streetAddress"] and addrData["streetAddress"]["line3"]:
            addrValue = addrData["streetAddress"]["line3"]
            jsonAddr[addrType + "ADDR_LINE3"] = addrValue
            fullAddress = (fullAddress + " " + addrValue).strip()
        if "line4" in addrData["streetAddress"] and addrData["streetAddress"]["line4"]:
            addrValue = addrData["streetAddress"]["line4"]
            jsonAddr[addrType + "ADDR_LINE4"] = addrValue
            fullAddress = (fullAddress + " " + addrValue).strip()

    # --never seen this populated, though have seen po boxes on line1
    if "postOfficeBox" in addrData and addrData["postOfficeBox"].get(
        "postOfficeBoxNumber"
    ):
        addrValue = (
            "Post Office Box " + addrData["postOfficeBox"]["postOfficeBoxNumber"]
        )
        jsonAddr[addrType + "ADDR_LINE1"] = addrValue
        fullAddress = "Post Office Box " + addrValue

    if (
        "addressLocality" in addrData
        and "name" in addrData["addressLocality"]
        and addrData["addressLocality"]["name"]
    ):
        addrValue = addrData["addressLocality"]["name"]
        jsonAddr[addrType + "ADDR_CITY"] = addrValue
        fullAddress = (fullAddress + " " + addrValue).strip()

    addrValue = ""
    if (
        "addressRegion" in addrData
        and "abbreviatedName" in addrData["addressRegion"]
        and addrData["addressRegion"]["abbreviatedName"]
    ):
        addrValue = addrData["addressRegion"]["abbreviatedName"]
    elif (
        "addressRegion" in addrData
        and "name" in addrData["addressRegion"]
        and addrData["addressRegion"]["name"]
    ):
        addrValue = addrData["addressRegion"]["name"]
    if addrValue:
        jsonAddr[addrType + "ADDR_STATE"] = addrValue
        fullAddress = (fullAddress + " " + addrValue).strip()

    if "postalCode" in addrData and addrData["postalCode"]:
        addrValue = addrData["postalCode"]
        jsonAddr[addrType + "ADDR_POSTAL_CODE"] = addrValue
        fullAddress = (fullAddress + " " + addrValue).strip()

    addrValue = ""
    if (
        "addressCountry" in addrData
        and "isoAlpha2Code" in addrData["addressCountry"]
        and addrData["addressCountry"]["isoAlpha2Code"]
    ):
        addrValue = addrData["addressCountry"]["isoAlpha2Code"]
    elif (
        "addressCountry" in addrData
        and "name" in addrData["addressCountry"]
        and addrData["addressCountry"]["name"]
    ):
        addrValue = addrData["addressCountry"]["name"]
    if addrValue:
        jsonAddr[addrType + "ADDR_COUNTRY"] = addrValue
        fullAddress = (fullAddress + " " + addrValue).strip()

    if checkit:
        print(json.dumps(addrData, indent=4))
        print("-" * 40)
        print(json.dumps(jsonAddr, indent=4))

    return fullAddress, jsonAddr


# ----------------------------------------
def format_GCA(rowData):

    # --data corrections / updates
    recordType = "PERSON"

    # --json header
    jsonData = {}
    jsonData["DATA_SOURCE"] = "DNB-CONTACT"
    jsonData["RECORD_ID"] = rowData["CONTACT_ID"]
    jsonData["RECORD_TYPE"] = recordType

    if rowData["INDIVIDUAL_ID"]:
        jsonData["DNB_CONTACT_ID"] = rowData["INDIVIDUAL_ID"]
        updateStat(recordType, "DNB_CONTACT_ID", rowData["INDIVIDUAL_ID"])

    # --map the name
    fullName = ""
    if rowData["NAMEPREFIX"]:
        jsonData["PRIMARY_NAME_PREFIX"] = rowData["NAMEPREFIX"]
        fullName += " " + rowData["NAMEPREFIX"]
    if rowData["FIRSTNAME"]:
        jsonData["PRIMARY_NAME_FIRST"] = rowData["FIRSTNAME"]
        fullName += " " + rowData["FIRSTNAME"]
    if rowData["MIDDLENAME"]:
        jsonData["PRIMARY_NAME_MIDDLE"] = rowData["MIDDLENAME"]
        fullName += " " + rowData["MIDDLENAME"]
    if rowData["LASTNAME"]:
        jsonData["PRIMARY_NAME_LAST"] = rowData["LASTNAME"]
        fullName += " " + rowData["LASTNAME"]
    if rowData["NAMESUFFIX"]:
        jsonData["PRIMARY_NAME_SUFFIX"] = rowData["NAMESUFFIX"]
        fullName += " " + rowData["NAMESUFFIX"]
    fullName = fullName.strip()
    if fullName:
        updateStat(recordType, "NAME-PRIMARY", fullName)

    # --add an aka name
    if rowData["GCA_NICKNAME"] and rowData["LASTNAME"]:
        jsonData["AKA_NAME_FIRST"] = rowData["GCA_NICKNAME"]
        jsonData["AKA_NAME_LAST"] = rowData["LASTNAME"]
        updateStat(recordType, "NAME-AKA", fullName)

    # --gender
    if rowData["GCA_GENDER"]:
        jsonData["GENDER"] = rowData["GCA_GENDER"]
        updateStat(recordType, "GENDER", rowData["GCA_GENDER"])

    # --map the address
    fullAddress = ""
    if rowData["GCA_STREETADDRESS1"]:
        jsonData["PRIMARY_ADDR_LINE1"] = rowData["GCA_STREETADDRESS1"]
        fullAddress += " " + rowData["GCA_STREETADDRESS1"]
    if rowData["GCA_STREETADDRESS2"]:
        jsonData["PRIMARY_ADDR_LINE2"] = rowData["GCA_STREETADDRESS2"]
        fullAddress += " " + rowData["GCA_STREETADDRESS2"]
    if rowData["GCA_CITYNAME"]:
        jsonData["PRIMARY_ADDR_CITY"] = rowData["GCA_CITYNAME"]
        fullAddress += " " + rowData["GCA_CITYNAME"]
    if rowData["GCA_STATEPROVINCECODE"]:
        jsonData["PRIMARY_ADDR_STATE"] = rowData["GCA_STATEPROVINCECODE"]
        fullAddress += " " + rowData["GCA_STATEPROVINCECODE"]
    if rowData["GCA_POSTALCODE"]:
        jsonData["PRIMARY_ADDR_POSTAL_CODE"] = rowData["GCA_POSTALCODE"]
        fullAddress += " " + rowData["GCA_POSTALCODE"]
    if rowData["GCA_COUNTRYCODE"]:
        jsonData["PRIMARY_ADDR_COUNTRY"] = rowData["GCA_COUNTRYCODE"]
        fullAddress += " " + rowData["GCA_COUNTRYCODE"]
    fullAddress = fullAddress.strip()
    if fullAddress:
        updateStat(recordType, "ADDRESS-PRIMARY", fullAddress)

    # --phones and email
    if rowData["PRIMARYPHONE"]:
        jsonData["PRIMARY_PHONE_NUMBER"] = rowData["PRIMARYPHONE"]
        updateStat(recordType, "PHONE-PRIMARY", rowData["PRIMARYPHONE"])
        if rowData["PRIMARYPHONEEXTENSION"]:
            jsonData["PRIMARY_PHONE_EXT"] = rowData["PRIMARYPHONEEXTENSION"]
    if rowData["SECONDARYPHONE"]:
        jsonData["SECONDARY_PHONE_NUMBER"] = rowData["SECONDARYPHONE"]
        updateStat(recordType, "PHONE-SECONDARY", rowData["SECONDARYPHONE"])
        if rowData["SECONDARYPHONEEXTENSION"]:
            jsonData["SECONDARY_PHONE_EXT"] = rowData["SECONDARYPHONEEXTENSION"]
    if rowData["EMAIL"]:
        jsonData["EMAIL_ADDRESS"] = rowData["EMAIL"]
        updateStat(recordType, "EMAIL_ADDRESS", rowData["EMAIL"])

    # --relate them to the company they own and use their group association for matching
    if rowData["DUNS_ID"]:
        jsonData["REL_POINTER_DOMAIN"] = "DUNS"
        jsonData["REL_POINTER_KEY"] = rowData["DUNS_ID"]
        jsonData["REL_POINTER_ROLE"] = "Contact"
        jsonData["GROUP_ASSN_ID_TYPE"] = "DUNS"
        jsonData["GROUP_ASSN_ID_NUMBER"] = rowData["DUNS_ID"]
        updateStat(recordType, "GROUP_ASSN_ID", rowData["DUNS_ID"])
    if rowData["GCA_BUSINESSNAME"]:
        jsonData["GROUP_ASSOCIATION_ORG_NAME"] = rowData["GCA_BUSINESSNAME"]
        updateStat(recordType, "GROUP_ASSOCIATION_NAME", rowData["GCA_BUSINESSNAME"])

    # --other info
    if rowData["JOBTITLE"]:
        jsonData["JOB_TITLE"] = rowData["JOBTITLE"]
        updateStat(recordType, "JOB_TITLE", rowData["JOBTITLE"])

    return [jsonData]  # --must return a list even though only 1


# ----------------------------------------
def format_UBO(rowData):

    # --gotta filter for this ... sometimes there is no ownership in a company
    # if not rowData['BENF_TYP_CD']:
    #    return []

    # --data corrections / updates
    if (
        ":" in rowData["SUBJ_DUNS"]
    ):  # --sometimes they prepended file name to first column like this: UBO_00_0819.txt:021475652
        rowData["SUBJ_DUNS"] = rowData["SUBJ_DUNS"][
            rowData["SUBJ_DUNS"].find(":") + 1 :
        ]

    if rowData["BENF_TYP_CD"] == "119":
        recordType = "PERSON"
        nameAttribute = "NAME_FULL"
        addressPrefix = ""
    else:
        recordType = "ORGANIZATION"
        nameAttribute = "NAME_ORG"
        addressPrefix = "BUSINESS_"

    # --json header
    jsonData = {}
    jsonData["DATA_SOURCE"] = "DNB-OWNER"
    if rowData["BENF_ID"]:
        jsonData["RECORD_ID"] = "%s-%s" % (rowData["SUBJ_DUNS"], rowData["BENF_ID"])
    jsonData["RECORD_TYPE"] = recordType
    updateStat("INPUT", recordType)

    jsonData[nameAttribute] = rowData["BENF_NME"]
    updateStat(recordType, nameAttribute, rowData["BENF_NME"])

    # --affiliate them to the subject country
    if rowData["SUBJ_CTRY_CD"]:
        jsonData["COUNTRY_OF_ASSOCIATION"] = rowData["SUBJ_CTRY_CD"]
        updateStat(recordType, "COUNTRY_OF_ASSOCIATION", rowData["SUBJ_CTRY_CD"])

    # --address
    addressData = {}
    addrFull = ""
    if rowData["BENF_ADR_LN1"]:
        addressData[addressPrefix + "ADDR_LINE1"] = rowData["BENF_ADR_LN1"]
        addrFull += " " + rowData["BENF_ADR_LN1"]
    if rowData["BENF_ADR_LN2"]:
        addressData[addressPrefix + "ADDR_LINE2"] = rowData["BENF_ADR_LN2"]
        addrFull += " " + rowData["BENF_ADR_LN2"]
    if rowData["BENF_ADR_LN3"]:
        addressData[addressPrefix + "ADDR_LINE3"] = rowData["BENF_ADR_LN3"]
        addrFull += " " + rowData["BENF_ADR_LN3"]
    if rowData["BENF_PRIM_TOWN"]:
        addressData[addressPrefix + "ADDR_CITY"] = rowData["BENF_PRIM_TOWN"]
        addrFull += " " + rowData["BENF_PRIM_TOWN"]
    if rowData["BENF_CNTY"] or rowData["BENF_PROV_OR_ST"]:
        addressData[addressPrefix + "ADDR_STATE"] = (
            rowData["BENF_CNTY"] + " " + rowData["BENF_PROV_OR_ST"]
        ).strip()
        addrFull += (
            " " + (rowData["BENF_CNTY"] + " " + rowData["BENF_PROV_OR_ST"]).strip()
        )
    if rowData["BENF_POST_CD"]:
        addressData[addressPrefix + "ADDR_POSTAL_CODE"] = rowData["BENF_POST_CD"]
        addrFull += " " + rowData["BENF_POST_CD"]
    if rowData["BENF_CTRY_CD"]:
        addressData[addressPrefix + "ADDR_COUNTRY"] = rowData["BENF_CTRY_CD"]
        addrFull += " " + rowData["BENF_CTRY_CD"]
    if addressData:
        jsonData.update(addressData)
        updateStat(recordType, "ADDRESS", addrFull.strip())

    # --these are good identifiers
    if rowData["BENF_DUNS"]:
        jsonData["DUNS_NUMBER"] = rowData["BENF_DUNS"]
        updateStat(recordType, "DUNS_NUMBER")
    if rowData["BENF_ID"]:
        jsonData["DNB_OWNER_ID"] = rowData["BENF_ID"]
        updateStat(recordType, "DNB_OWNER_ID")

    # --these aren't currently populated but maybe one day!
    if rowData["NATY"]:
        jsonData["NATIONALITY"] = rowData["NATY"]
        updateStat(recordType, "NATIONALITY", rowData["NATY"])
    if rowData["DT_OF_BRTH"]:
        jsonData["DATE_OF_BIRTH"] = rowData["DT_OF_BRTH"]
        updateStat(recordType, "DATE_OF_BIRTH", rowData["DT_OF_BRTH"])

    # --relate them to the company they own and use their group association for matching
    if rowData["SUBJ_DUNS"]:
        jsonData["REL_POINTER_DOMAIN"] = "DUNS"
        jsonData["REL_POINTER_KEY"] = rowData["SUBJ_DUNS"]
        jsonData["REL_POINTER_ROLE"] = "Owner"
        jsonData["GROUP_ASSN_ID_TYPE"] = "DUNS"
        jsonData["GROUP_ASSN_ID_NUMBER"] = rowData["SUBJ_DUNS"]
        updateStat(recordType, "GROUP_ASSN_ID", rowData["SUBJ_DUNS"])
    if rowData["SUBJ_NME"]:
        jsonData["GROUP_ASSOCIATION_ORG_NAME"] = rowData["SUBJ_NME"]
        updateStat(recordType, "GROUP_ASSOCIATION_NAME", rowData["SUBJ_NME"])

    # --additional useful information
    if rowData["BENF_LGL_FORM_DESC"]:
        jsonData["LEGAL_FORM"] = rowData["BENF_LGL_FORM_DESC"]
        updateStat(recordType, "LEGAL_FORM", rowData["BENF_LGL_FORM_DESC"])
    if rowData["DIRC_OWRP_PCTG"]:
        jsonData["DIRECT_OWNERSHIP_PERCENT"] = float(rowData["DIRC_OWRP_PCTG"])
        updateStat(recordType, "DIRECT_OWNERSHIP_PERCENT", rowData["DIRC_OWRP_PCTG"])
        jsonData["REL_POINTER_ROLE"] += " %sD" % rowData["DIRC_OWRP_PCTG"]
    if rowData["IDIR_OWRP_PCTG"]:
        jsonData["INDIRECT_OWNERSHIP_PERCENT"] = float(rowData["IDIR_OWRP_PCTG"])
        jsonData["REL_POINTER_ROLE"] += " %sI" % rowData["IDIR_OWRP_PCTG"]
        updateStat(recordType, "INDIRECT_OWNERSHIP_PERCENT", rowData["IDIR_OWRP_PCTG"])
    if rowData["BENF_OWRP_PCTG"]:
        jsonData["BENEFICIAL_OWNERSHIP_PERCENT"] = float(rowData["BENF_OWRP_PCTG"])
        jsonData["REL_POINTER_ROLE"] += " %sB" % rowData["BENF_OWRP_PCTG"]
        updateStat(
            recordType, "BENEFICIAL_OWNERSHIP_PERCENT", rowData["BENF_OWRP_PCTG"]
        )

    return [jsonData]  # --must return a list even though only 1


# ----------------------------------------
def format_UBO2(rowData, ubo_depth_cache):

    # --gotta filter for this as status has too many unknown values
    if not rowData["BENF_NME"]:
        return [], ubo_depth_cache

    # --assume a depth of 1 if not populated
    if not rowData["DEPTH"]:
        rowData["DEPTH"] = 1

    # --data corrections / updates
    if (
        ":" in rowData["SUBJ_DUNS"]
    ):  # --sometimes they prepended file name to first column like this: UBO_00_0819.txt:021475652
        rowData["SUBJ_DUNS"] = rowData["SUBJ_DUNS"][
            rowData["SUBJ_DUNS"].find(":") + 1 :
        ]

    if rowData["BENF_TYP_CD"] == "119":
        recordType = "PERSON"
        nameAttribute = "NAME_FULL"
        addressPrefix = ""
    else:
        recordType = "ORGANIZATION"
        nameAttribute = "NAME_ORG"
        addressPrefix = "BUSINESS_"

    # --json header
    jsonData = {}
    jsonData["DATA_SOURCE"] = "DNB-OWNER"
    if rowData["BENF_ID"]:
        jsonData["RECORD_ID"] = "%s-%s" % (rowData["SUBJ_DUNS"], rowData["BENF_ID"])
    jsonData["RECORD_TYPE"] = recordType
    updateStat("INPUT", recordType)

    jsonData[nameAttribute] = rowData["BENF_NME"]
    updateStat(recordType, nameAttribute, rowData["BENF_NME"])

    # --affiliate them to the subject country
    if rowData["SUBJ_CTRY_CD"]:
        jsonData["COUNTRY_OF_ASSOCIATION"] = rowData["SUBJ_CTRY_CD"]
        updateStat(recordType, "COUNTRY_OF_ASSOCIATION", rowData["SUBJ_CTRY_CD"])

    # --address
    addressData = {}
    addrFull = ""
    if rowData["BENF_ADR_LN1"]:
        addressData[addressPrefix + "ADDR_LINE1"] = rowData["BENF_ADR_LN1"]
        addrFull += " " + rowData["BENF_ADR_LN1"]
    if rowData["BENF_ADR_LN2"]:
        addressData[addressPrefix + "ADDR_LINE2"] = rowData["BENF_ADR_LN2"]
        addrFull += " " + rowData["BENF_ADR_LN2"]
    if rowData["BENF_ADR_LN3"]:
        addressData[addressPrefix + "ADDR_LINE3"] = rowData["BENF_ADR_LN3"]
        addrFull += " " + rowData["BENF_ADR_LN3"]
    if rowData["BENF_PRIM_TOWN"]:
        addressData[addressPrefix + "ADDR_CITY"] = rowData["BENF_PRIM_TOWN"]
        addrFull += " " + rowData["BENF_PRIM_TOWN"]
    if rowData["BENF_CNTY"] or rowData["BENF_PROV_OR_ST"]:
        addressData[addressPrefix + "ADDR_STATE"] = (
            rowData["BENF_CNTY"] + " " + rowData["BENF_PROV_OR_ST"]
        ).strip()
        addrFull += (
            " " + (rowData["BENF_CNTY"] + " " + rowData["BENF_PROV_OR_ST"]).strip()
        )
    if rowData["BENF_POST_CD"]:
        addressData[addressPrefix + "ADDR_POSTAL_CODE"] = rowData["BENF_POST_CD"]
        addrFull += " " + rowData["BENF_POST_CD"]
    if rowData["BENF_CTRY_CD"]:
        addressData[addressPrefix + "ADDR_COUNTRY"] = rowData["BENF_CTRY_CD"]
        addrFull += " " + rowData["BENF_CTRY_CD"]
    if addressData:
        jsonData.update(addressData)
        updateStat(recordType, "ADDRESS", addrFull.strip())

    # --these are good identifiers
    if rowData["BENF_DUNS"]:
        jsonData["DUNS_NUMBER"] = rowData["BENF_DUNS"]
        updateStat(recordType, "DUNS_NUMBER")
    if rowData["BENF_ID"]:
        jsonData["DNB_OWNER_ID"] = rowData["BENF_ID"]
        updateStat(recordType, "DNB_OWNER_ID")

    # --these aren't currently populated but maybe one day!
    if rowData["NATY"]:
        jsonData["NATIONALITY"] = rowData["NATY"]
        updateStat(recordType, "NATIONALITY", rowData["NATY"])
    if rowData["DT_OF_BRTH"]:
        jsonData["DATE_OF_BIRTH"] = rowData["DT_OF_BRTH"]
        updateStat(recordType, "DATE_OF_BIRTH", rowData["DT_OF_BRTH"])

    # --additional useful information
    ownershipLabel = "Owns"
    if rowData["BENF_LGL_FORM_DESC"]:
        jsonData["LEGAL_FORM"] = rowData["BENF_LGL_FORM_DESC"]
        updateStat(recordType, "LEGAL_FORM", rowData["BENF_LGL_FORM_DESC"])
    if rowData["DIRC_OWRP_PCTG"]:
        jsonData["DIRECT_OWNERSHIP_PERCENT"] = float(rowData["DIRC_OWRP_PCTG"])
        updateStat(recordType, "DIRECT_OWNERSHIP_PERCENT", rowData["DIRC_OWRP_PCTG"])
        ownershipLabel += " %sD" % rowData["DIRC_OWRP_PCTG"]
    if rowData["IDIR_OWRP_PCTG"]:
        jsonData["INDIRECT_OWNERSHIP_PERCENT"] = float(rowData["IDIR_OWRP_PCTG"])
        ownershipLabel += " %sI" % rowData["IDIR_OWRP_PCTG"]
        updateStat(recordType, "INDIRECT_OWNERSHIP_PERCENT", rowData["IDIR_OWRP_PCTG"])
    if rowData["BENF_OWRP_PCTG"]:
        jsonData["BENEFICIAL_OWNERSHIP_PERCENT"] = float(rowData["BENF_OWRP_PCTG"])
        ownershipLabel += " %sB" % rowData["BENF_OWRP_PCTG"]
        updateStat(
            recordType, "BENEFICIAL_OWNERSHIP_PERCENT", rowData["BENF_OWRP_PCTG"]
        )

    # --start the depth chart for the subject duns
    if rowData["SUBJ_DUNS"] not in ubo_depth_cache:
        ubo_depth_cache[rowData["SUBJ_DUNS"]] = {}
        ubo_depth_cache[rowData["SUBJ_DUNS"]][0] = [rowData["SUBJ_DUNS"]]
    currentDepth = int(rowData["DEPTH"])
    if currentDepth not in ubo_depth_cache[rowData["SUBJ_DUNS"]]:
        ubo_depth_cache[rowData["SUBJ_DUNS"]][currentDepth] = []
    if rowData["BENF_DUNS"]:
        ubo_depth_cache[rowData["SUBJ_DUNS"]][currentDepth].append(rowData["BENF_DUNS"])

    relationshipList = []

    # --beneficiaries may point to this one, based on the depth
    if rowData["BENF_DUNS"]:
        relationshipList.append(
            {"REL_ANCHOR_DOMAIN": "DUNS", "REL_ANCHOR_KEY": rowData["BENF_DUNS"]}
        )

    # --determine who this owner should point to
    if currentDepth - 1 in ubo_depth_cache[rowData["SUBJ_DUNS"]]:
        # print(ubo_depth_cache[rowData['SUBJ_DUNS']])
        # print(json.dumps(rowData, indent=4))

        for related_duns in ubo_depth_cache[rowData["SUBJ_DUNS"]][currentDepth - 1]:
            relationshipList.append(
                {
                    "REL_POINTER_DOMAIN": "DUNS",
                    "REL_POINTER_KEY": related_duns,
                    "REL_POINTER_ROLE": ownershipLabel,
                }
            )
    else:
        relationshipList.append(
            {
                "REL_POINTER_DOMAIN": "DUNS",
                "REL_POINTER_KEY": rowData["SUBJ_DUNS"],
                "REL_POINTER_ROLE": ownershipLabel,
            }
        )
        # if currentDepth > 1

    jsonData["RELATIONSHIPS"] = relationshipList

    # --relate persons to the company they own and use their group association for matching
    if recordType == "PERSON":
        if rowData["SUBJ_DUNS"]:
            jsonData["GROUP_ASSN_ID_TYPE"] = "DUNS"
            jsonData["GROUP_ASSN_ID_NUMBER"] = rowData["SUBJ_DUNS"]
            updateStat(recordType, "GROUP_ASSN_ID", rowData["SUBJ_DUNS"])
        if rowData["SUBJ_NME"]:
            jsonData["GROUP_ASSOCIATION_ORG_NAME"] = rowData["SUBJ_NME"]
            updateStat(recordType, "GROUP_ASSOCIATION_NAME", rowData["SUBJ_NME"])

    return [jsonData], ubo_depth_cache  # --must return a list even though only 1


# ----------------------------------------
def format_UBO_SUBJECT(rowData, ubo_company_cache):

    # --data corrections / updates
    if (
        ":" in rowData["SUBJ_DUNS"]
    ):  # --sometimes they prepended file name to first column like this: UBO_00_0819.txt:021475652
        rowData["SUBJ_DUNS"] = rowData["SUBJ_DUNS"][
            rowData["SUBJ_DUNS"].find(":") + 1 :
        ]

    if "subject" not in ubo_company_cache:
        ubo_company_cache["subject"] = {}
    if "parent" not in ubo_company_cache:
        ubo_company_cache["parent"] = {}

    # --bypass if already mapped
    if rowData["SUBJ_DUNS"] in ubo_company_cache["subject"]:
        updateStat("DUPLICATE", "SUBJECT_DUNS", rowData["SUBJ_DUNS"])
        return [], ubo_company_cache
    ubo_company_cache["subject"][rowData["SUBJ_DUNS"]] = 1

    jsonList = []

    # --subject company json
    jsonData = {}
    jsonData["DATA_SOURCE"] = "DNB-COMPANY"
    jsonData["RECORD_ID"] = rowData["SUBJ_DUNS"]
    jsonData["DUNS_NUMBER"] = rowData["SUBJ_DUNS"]
    jsonData["RECORD_TYPE"] = "ORGANIZATION"
    updateStat("DATA_SOURCE", "DNB-COMPANY")

    jsonData["PRIMARY_NAME_ORG"] = rowData["SUBJ_NME"]
    jsonData["BUSINESS_ADDR_LINE1"] = rowData["SUBJ_ADR_LN1"]
    jsonData["BUSINESS_ADDR_LINE2"] = rowData["SUBJ_ADR_LN2"]
    jsonData["BUSINESS_ADDR_LINE3"] = rowData["SUBJ_ADR_LN3"]
    jsonData["BUSINESS_ADDR_CITY"] = rowData["SUBJ_PRIM_TOWN"]
    jsonData["BUSINESS_ADDR_STATE"] = rowData["SUBJ_PROV_OR_ST"]
    jsonData["BUSINESS_ADDR_POSTAL_CODE"] = rowData["SUBJ_POST_CD"]
    jsonData["BUSINESS_ADDR_COUNTRY"] = rowData["SUBJ_CTRY_CD"]
    jsonData["legal_form"] = rowData["SUBJ_LGL_FORM_DESC"]
    jsonData["sic_code"] = "%s-%s" % (rowData["SIC_CD"], rowData["SIC_CD_DESC"])

    relationshipList = []
    relData = {}
    relData["REL_ANCHOR_DOMAIN"] = "DUNS"
    relData["REL_ANCHOR_KEY"] = rowData["SUBJ_DUNS"]
    relationshipList.append(relData)
    if rowData["PRNT_DUNS"]:
        relData = {}
        relData["REL_POINTER_DOMAIN"] = "DUNS"
        relData["REL_POINTER_KEY"] = rowData["PRNT_DUNS"]
        relData["REL_POINTER_ROLE"] = "direct parent"
        relationshipList.append(relData)
    if rowData["DOM_ULT_DUNS"]:
        relData = {}
        relData["REL_POINTER_DOMAIN"] = "DUNS"
        relData["REL_POINTER_KEY"] = rowData["DOM_ULT_DUNS"]
        relData["REL_POINTER_ROLE"] = "domestic parent"
        relationshipList.append(relData)
    if rowData["GLBL_ULT_DUNS"]:
        relData = {}
        relData["REL_POINTER_DOMAIN"] = "DUNS"
        relData["REL_POINTER_KEY"] = rowData["GLBL_ULT_DUNS"]
        relData["REL_POINTER_ROLE"] = "global parent"
        relationshipList.append(relData)
    jsonData["RELATIONSHIP_LIST"] = relationshipList
    jsonList.append(jsonData)

    if rowData["PRNT_DUNS"] and rowData["PRNT_DUNS"] not in ubo_company_cache["parent"]:
        ubo_company_cache["parent"][rowData["PRNT_DUNS"]] = 1
        jsonData = {}
        jsonData["DATA_SOURCE"] = "DNB-PARENT"
        jsonData["RECORD_ID"] = rowData["PRNT_DUNS"]
        jsonData["DUNS_NUMBER"] = rowData["PRNT_DUNS"]
        jsonData["RECORD_TYPE"] = "ORGANIZATION"
        jsonData["PRIMARY_NAME_ORG"] = rowData["PRNT_NME"]
        jsonData["REL_ANCHOR_DOMAIN"] = "DUNS"
        jsonData["REL_ANCHOR_KEY"] = rowData["PRNT_DUNS"]
        updateStat("DATA_SOURCE", "DNB-PARENT")
        jsonList.append(jsonData)

    if (
        rowData["DOM_ULT_DUNS"]
        and rowData["DOM_ULT_DUNS"] not in ubo_company_cache["parent"]
    ):
        ubo_company_cache["parent"][rowData["DOM_ULT_DUNS"]] = 1
        jsonData = {}
        jsonData["DATA_SOURCE"] = "DNB-PARENT"
        jsonData["RECORD_ID"] = rowData["DOM_ULT_DUNS"]
        jsonData["DUNS_NUMBER"] = rowData["DOM_ULT_DUNS"]
        jsonData["RECORD_TYPE"] = "ORGANIZATION"
        jsonData["PRIMARY_NAME_ORG"] = rowData["DOM_ULT_NME"]
        jsonData["REL_ANCHOR_DOMAIN"] = "DUNS"
        jsonData["REL_ANCHOR_KEY"] = rowData["DOM_ULT_DUNS"]
        updateStat("DATA_SOURCE", "DNB-PARENT")
        jsonList.append(jsonData)

    if (
        rowData["GLBL_ULT_DUNS"]
        and rowData["GLBL_ULT_DUNS"] not in ubo_company_cache["parent"]
    ):
        ubo_company_cache["parent"][rowData["GLBL_ULT_DUNS"]] = 1
        jsonData = {}
        jsonData["DATA_SOURCE"] = "DNB-PARENT"
        jsonData["RECORD_ID"] = rowData["GLBL_ULT_DUNS"]
        jsonData["DUNS_NUMBER"] = rowData["GLBL_ULT_DUNS"]
        jsonData["RECORD_TYPE"] = "ORGANIZATION"
        jsonData["PRIMARY_NAME_ORG"] = rowData["GLBL_ULT_NME"]
        jsonData["REL_ANCHOR_DOMAIN"] = "DUNS"
        jsonData["REL_ANCHOR_KEY"] = rowData["GLBL_ULT_DUNS"]
        updateStat("DATA_SOURCE", "DNB-PARENT")
        jsonList.append(jsonData)

    return jsonList, ubo_company_cache


# ----------------------------------------
def pause(question="PRESS ENTER TO CONTINUE ..."):
    """pause for debug purposes"""
    try:
        response = input(question)
    except KeyboardInterrupt:
        response = None
        global shutDown
        shutDown = True
    return response


# ----------------------------------------
def signal_handler(signal, frame):
    print("USER INTERRUPT! Shutting down ... (please wait)")
    global shutDown
    shutDown = True
    return


# ----------------------------------------
def updateStat(cat1, cat2, example=None):

    if cat1 not in statPack:
        statPack[cat1] = {}
    if cat2 not in statPack[cat1]:
        statPack[cat1][cat2] = {}
        statPack[cat1][cat2]["count"] = 0

    statPack[cat1][cat2]["count"] += 1
    if example:
        if "examples" not in statPack[cat1][cat2]:
            statPack[cat1][cat2]["examples"] = []
        if example not in statPack[cat1][cat2]["examples"]:
            if len(statPack[cat1][cat2]["examples"]) < 5:
                statPack[cat1][cat2]["examples"].append(example)
            else:
                randomSampleI = random.randint(2, 4)
                statPack[cat1][cat2]["examples"][randomSampleI] = example
    return


# ----------------------------------------
if __name__ == "__main__":

    appPath = os.path.dirname(os.path.abspath(sys.argv[0]))

    global shutDown
    shutDown = False
    signal.signal(signal.SIGINT, signal_handler)

    global statPack
    statPack = {}

    procStartTime = time.time()
    progressInterval = 10000

    # --load the dnb file formats
    dnbFormatFile = appPath + os.path.sep + "dnb_formats.json"
    print(f"\nFormats File: {dnbFormatFile}")

    if not os.path.exists(dnbFormatFile):
        print(f"\nFormat file missing: {dnbFormatFile}\n")
        sys.exit(1)

    try:
        dnbFormats = json.load(open(dnbFormatFile, "r"))
    except json.decoder.JSONDecodeError as err:
        print(f"\nJSON error {err} in {dnbFormatFile}\n")
        sys.exit(1)

    argparser = argparse.ArgumentParser()
    argparser.add_argument(
        "-f",
        "--dnb_format",
        default=os.getenv("dnb_format".upper(), None),
        type=str.upper,
        help="choose CMPCVF, GCA, UBO or UBO_ALONE",
    )
    argparser.add_argument(
        "-i",
        "--input_spec",
        default=os.getenv("input_spec".upper(), None),
        type=str,
        help="the name of one or more DNB files to map (place in quotes if you use wild cards)",
    )
    argparser.add_argument(
        "-o",
        "--output_path",
        default=os.getenv("output_path".upper(), None),
        type=str,
        help="output directory or file name for mapped json records",
    )
    argparser.add_argument(
        "-l",
        "--log_file",
        default=os.getenv("log_file", None),
        type=str,
        help="optional statistics filename (json format).",
    )
    args = argparser.parse_args()
    outputFilePath = args.output_path
    logFile = args.log_file

    # --verify dnb format code
    if not args.dnb_format:
        print(f"\nPlease select a DNB format code from {dnbFormatFile}\n")
        sys.exit(1)

    dnbFormat = args.dnb_format.upper()
    if dnbFormat not in dnbFormats["mappings"]:
        print(f"\nDNB format code {dnbFormat} not found in dnb_formats.json\n")
        sys.exit(1)

    # --verify input files to process
    if not args.input_spec:
        print("\nPlease enter one or morefile(s) to process\n")
        sys.exit(1)

    inputFileList = glob.glob(args.input_spec)
    if len(inputFileList) == 0:
        print(f"\nNo files found matching {args.input_spec}\n")
        sys.exit(1)

    # --open output if a single file was specified
    if not outputFilePath:
        print("\nPlease enter a directory or file to write the output files to\n")
        sys.exit(1)

    outputIsFile = not os.path.isdir(outputFilePath)
    if outputIsFile:
        try:
            outputFileHandle = open(outputFilePath, "w", encoding="utf-8")
        except IOError as err:
            print(f"\nCould not open output file {outputFilePath} for writing: {err}\n")
            sys.exit(1)
    else:
        if outputFilePath[-1] != os.path.sep:
            outputFilePath += os.path.sep

    # --initialize some stats
    statPack = {}

    # --for each input file
    inputFileNum = 0
    for inputFileName in sorted(inputFileList):
        inputFileNum += 1
        fileDisplay = f"Processing file {inputFileNum} of {len(inputFileList)} - {inputFileName}...\n"
        print(f"\n" + "-" * len(fileDisplay))
        print(fileDisplay)

        shutDown = processFile(inputFileName)
        if shutDown:
            break

    print(f"\n{inputFileNum} of {len(inputFileList)} files processed")

    if outputIsFile:
        outputFileHandle.close()

    # --write statistics file
    if logFile:
        with open(logFile, "w") as outfile:
            json.dump(statPack, outfile, indent=4, sort_keys=True)
        print(f"\nMapping stats written to {logFile}")

    elapsedMins = round((time.time() - procStartTime) / 60, 1)
    success_msg = "completed successfully in" if shutDown == 0 else "aborted after"
    print(f"\nProcess {success_msg} {elapsedMins} minutes")

    sys.exit(0)
