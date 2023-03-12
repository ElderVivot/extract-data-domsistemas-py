SELECT  STRING(emp.codi_emp) AS codeCompanieAccountSystem,
        emp.nome_emp AS name,
        emp.apel_emp AS nickName,
        emp.tins_emp AS typeFederalRegistration,
        COALESCE(TRIM(emp.cgce_emp), '') AS federalRegistration,
        emp.stat_emp AS status,
        STRING(emp.dddf_emp) AS dddPhone,
        emp.fone_emp AS phone,
        emp.email_emp AS email,
        emp.dtinicio_emp AS dateInicialAsCompanie,
        emp.dcad_emp AS dateInicialAsClient,
        emp.dina_emp AS dateFinalAsClient,
        emp.iest_emp AS stateRegistration,
        emp.imun_emp AS cityRegistration,
        emp.esta_emp AS uf,
        COALESCE(emp.i_cnae20, '') || ',' ||
            COALESCE( ( SELECT LIST(cnae_sec.codigo_cnae)
                        FROM bethadba.geatvsecundaria AS cnae_sec
                        WHERE cnae_sec.codi_emp = emp.codi_emp ), '' ) AS cnaes, 
        COALESCE( ( SELECT STRING(mun.codigo_ibge)
                    FROM bethadba.gemunicipio AS mun
                    WHERE mun.codigo_municipio = emp.codigo_municipio ) , '' ) AS idIbgeCity,
        COALESCE( (SELECT STRING(vig.RFED_PAR)
            FROM bethadba.EFPARAMETRO_VIGENCIA AS vig
            WHERE vig.CODI_EMP = emp.codi_emp 
                AND vig.VIGENCIA_PAR = (SELECT MAX(vig2.VIGENCIA_PAR )
                                        FROM bethadba.EFPARAMETRO_VIGENCIA AS vig2
                                        WHERE vig2.codi_emp = emp.codi_emp 
                                        AND vig2.VIGENCIA_PAR <= today() )), '99' ) AS taxRegime

    FROM bethadba.geempre AS emp

ORDER BY emp.codi_emp