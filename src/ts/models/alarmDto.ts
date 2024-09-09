/**
 * Alarmservice Demo
 * No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)
 *
 * The version of the OpenAPI document: 1.0.0
 * 
 *
 * NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).
 * https://openapi-generator.tech
 * Do not edit the class manually.
 */



export class AlarmDto {
    'reason'?: string;
    'acknowledged'?: boolean;
    'timestamp'?: string;
    'alarmId'?: number;
    'roomId'?: number;

    static discriminator: string | undefined = undefined;

    static attributeTypeMap: Array<{name: string, baseName: string, type: string}> = [
        {
            "name": "reason",
            "baseName": "reason",
            "type": "string"
        },
        {
            "name": "acknowledged",
            "baseName": "acknowledged",
            "type": "boolean"
        },
        {
            "name": "timestamp",
            "baseName": "timestamp",
            "type": "string"
        },
        {
            "name": "alarmId",
            "baseName": "alarmId",
            "type": "number"
        },
        {
            "name": "roomId",
            "baseName": "roomId",
            "type": "number"
        }    ];

    static getAttributeTypeMap() {
        return AlarmDto.attributeTypeMap;
    }
}

