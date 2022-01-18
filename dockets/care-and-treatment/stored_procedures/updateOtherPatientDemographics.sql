CREATE  PROCEDURE `updateOtherPatientDemographics`(
IN startDate Date,
IN endDate Date
)
BEGIN
 set @startDate:=startDate;
 set @endDate:=endDate;
 delete from ndwr.other_patient_details_2;
 replace into ndwr.other_patient_details_2(person_id,
    last_number_of_siblings,
    last_marital_status,
    last_occupation,
    last_nutrition_status,
    last_child_disclosure_status,
    last_discordant_couple,
    last_education_level) SELECT DISTINCT
    person_id,
    last_number_of_siblings,
    last_marital_status,
    last_occupation,
    last_nutrition_status,
    last_child_disclosure_status,
    last_discordant_couple,
    last_education_level
FROM
    (SELECT 
        @prev_id:=@cur_id AS prev_id,
            @cur_id:=t1.person_id AS cur_id,
            t1.person_id,
            CASE
                WHEN
                    t1.education_level = 1175
                        OR t1.education_level = 1067
                THEN
                    @education_level:=NULL
                WHEN
                    @prev_id != @cur_id
                        AND t1.education_level
                THEN
                    @education_level:=t1.education_level
                WHEN
                    @prev_id != @cur_id
                        AND (t1.education_level IS NULL)
                THEN
                    @education_level:=t1.education_level
                WHEN
                    @prev_id = @cur_id
                        AND t1.education_level
                        AND @education_level IS NULL
                THEN
                    @education_level:=t1.education_level
                ELSE @education_level
            END AS last_education_level,
            CASE
                WHEN
                    t1.occupation = 1175
                        OR t1.occupation = 1067
                THEN
                    @occupation:=NULL
                WHEN @prev_id != @cur_id AND t1.occupation THEN @occupation:=t1.occupation
                WHEN
                    @prev_id != @cur_id
                        AND (t1.occupation IS NULL)
                THEN
                    @occupation:=t1.occupation
                WHEN
                    @prev_id = @cur_id AND t1.occupation
                        AND @occupation IS NULL
                THEN
                    @occupation:=t1.occupation
                ELSE @occupation
            END AS last_occupation,
            CASE
                WHEN
                    t1.marital_status = 1175
                        OR t1.marital_status = 1067
                THEN
                    @marital_status:=NULL
                WHEN
                    @prev_id != @cur_id
                        AND t1.marital_status
                THEN
                    @marital_status:=t1.marital_status
                WHEN
                    @prev_id != @cur_id
                        AND (t1.marital_status IS NULL)
                THEN
                    @marital_status:=t1.marital_status
                WHEN
                    @prev_id = @cur_id AND t1.marital_status
                        AND @marital_status IS NULL
                THEN
                    @marital_status:=t1.marital_status
                ELSE @marital_status
            END AS last_marital_status,
            CASE
                WHEN
                    @prev_id != @cur_id
                        AND t1.number_of_siblings
                THEN
                    @number_of_siblings:=t1.number_of_siblings
                WHEN
                    @prev_id != @cur_id
                        AND (t1.number_of_siblings IS NULL)
                THEN
                    @number_of_siblings:=t1.number_of_siblings
                WHEN
                    @prev_id = @cur_id
                        AND t1.number_of_siblings
                        AND @number_of_siblings IS NULL
                THEN
                    @number_of_siblings:=t1.number_of_siblings
                ELSE @number_of_siblings
            END AS last_number_of_siblings,
            CASE
                WHEN
                    t1.nutrition_status = 1175
                        OR t1.nutrition_status = 1067
                THEN
                    @nutrition_status:=NULL
                WHEN
                    @prev_id != @cur_id
                        AND t1.nutrition_status
                THEN
                    @nutrition_status:=t1.nutrition_status
                WHEN
                    @prev_id != @cur_id
                        AND (t1.nutrition_status IS NULL)
                THEN
                    @nutrition_status:=t1.nutrition_status
                WHEN
                    @prev_id = @cur_id
                        AND t1.nutrition_status
                        AND @nutrition_status IS NULL
                THEN
                    @nutrition_status:=t1.nutrition_status
                ELSE @nutrition_status
            END AS last_nutrition_status,
            CASE
                WHEN
                    t1.child_disclosure_status = 1175
                        OR t1.child_disclosure_status = 1067
                THEN
                    @child_disclosure_status:=NULL
                WHEN
                    @prev_id != @cur_id
                        AND t1.child_disclosure_status
                THEN
                    @child_disclosure_status:=t1.child_disclosure_status
                WHEN
                    @prev_id != @cur_id
                        AND (t1.child_disclosure_status IS NULL)
                THEN
                    @child_disclosure_status:=t1.child_disclosure_status
                WHEN
                    @prev_id = @cur_id
                        AND t1.child_disclosure_status
                        AND @child_disclosure_status IS NULL
                THEN
                    @child_disclosure_status:=t1.child_disclosure_status
                ELSE @child_disclosure_status
            END AS last_child_disclosure_status,
            CASE
                WHEN
                    t1.discordant_couple = 1175
                        OR t1.discordant_couple = 1067
                THEN
                    @discordant_couple:=NULL
                WHEN
                    @prev_id != @cur_id
                        AND t1.discordant_couple
                THEN
                    @discordant_couple:=t1.discordant_couple
                WHEN
                    @prev_id != @cur_id
                        AND (t1.discordant_couple IS NULL)
                THEN
                    @discordant_couple:=t1.discordant_couple
                WHEN
                    @prev_id = @cur_id
                        AND t1.discordant_couple
                        AND @discordant_couple IS NULL
                THEN
                    @discordant_couple:=t1.discordant_couple
                ELSE @discordant_couple
            END AS last_discordant_couple
    FROM
        (SELECT 
        person_id,
            encounter_datetime,
            IF(obs REGEXP '!!1605=', REPLACE(REPLACE((SUBSTRING_INDEX(SUBSTRING(obs, LOCATE('!!1605=', obs)), '##', ROUND((LENGTH(obs) - LENGTH(REPLACE(obs, '!!1605=', ''))) / LENGTH('!!1605=')))), '!!1605=', ''), '!!', ''), NULL) AS education_level,
            IF(obs REGEXP '!!1054=', REPLACE(REPLACE((SUBSTRING_INDEX(SUBSTRING(obs, LOCATE('!!1054=', obs)), '##', ROUND((LENGTH(obs) - LENGTH(REPLACE(obs, '!!1054=', ''))) / LENGTH('!!1054=')))), '!!1054=', ''), '!!', ''), NULL) AS marital_status,
            IF(obs REGEXP '!!1972=', REPLACE(REPLACE((SUBSTRING_INDEX(SUBSTRING(obs, LOCATE('!!1972=', obs)), '##', ROUND((LENGTH(obs) - LENGTH(REPLACE(obs, '!!1972=', ''))) / LENGTH('!!1972=')))), '!!1972=', ''), '!!', ''), NULL) AS occupation,
            IF(obs REGEXP '!!5573=', REPLACE(REPLACE((SUBSTRING_INDEX(SUBSTRING(obs, LOCATE('!!5573=', obs)), '##', ROUND((LENGTH(obs) - LENGTH(REPLACE(obs, '!!5573=', ''))) / LENGTH('!!5573=')))), '!!5573=', ''), '!!', ''), NULL) AS number_of_siblings,
            IF(obs REGEXP '!!6596=', REPLACE(REPLACE((SUBSTRING_INDEX(SUBSTRING(obs, LOCATE('!!6596=', obs)), '##', ROUND((LENGTH(obs) - LENGTH(REPLACE(obs, '!!6596=', ''))) / LENGTH('!!6596=')))), '!!6596=', ''), '!!', ''), NULL) AS child_disclosure_status,
            IF(obs REGEXP '!!7369=', REPLACE(REPLACE((SUBSTRING_INDEX(SUBSTRING(obs, LOCATE('!!7369=', obs)), '##', ROUND((LENGTH(obs) - LENGTH(REPLACE(obs, '!!7369=', ''))) / LENGTH('!!7369=')))), '!!7369=', ''), '!!', ''), NULL) AS nutrition_status,
            IF(obs REGEXP '!!6096=', REPLACE(REPLACE((SUBSTRING_INDEX(SUBSTRING(obs, LOCATE('!!6096=', obs)), '##', ROUND((LENGTH(obs) - LENGTH(REPLACE(obs, '!!6096=', ''))) / LENGTH('!!6096=')))), '!!6096=', ''), '!!', ''), NULL) AS discordant_couple
    FROM
        etl.flat_obs
    WHERE
    encounter_datetime >= @startDate            AND encounter_datetime <= @endDate   AND 
    obs REGEXP '!!1605=|!!6596=|!!7369=|!!1972=|!!1054=|!!5573=|!!6096='
    ORDER BY person_id , encounter_datetime DESC) t1) s
    where last_discordant_couple<>'' or last_number_of_siblings <>'' or 
    last_marital_status <>'' or 
    last_occupation <>'' or 
    last_nutrition_status <>'' or 
    last_child_disclosure_status <>'';
 
 END