SELECT STRING(emp.codi_emp) AS codeCompanieAccountSystem,
    emp.nome_emp AS name,
    emp.tins_emp AS typeFederalRegistration,
    COALESCE(TRIM(emp.cgce_emp), '') AS federalRegistration,
    emp.stat_emp AS status,
    emp.dtinicio_emp,
    COALESCE(
        (
            SELECT STRING(vig.RFED_PAR)
            FROM bethadba.EFPARAMETRO_VIGENCIA AS vig
            WHERE vig.CODI_EMP = emp.codi_emp
                AND vig.VIGENCIA_PAR = (
                    SELECT MAX(vig2.VIGENCIA_PAR)
                    FROM bethadba.EFPARAMETRO_VIGENCIA AS vig2
                    WHERE vig2.codi_emp = emp.codi_emp
                        AND vig2.VIGENCIA_PAR <= DATE('#competence#')
                )
        ),
        '99'
    ) AS taxRegime
FROM bethadba.geempre AS emp
WHERE emp.stat_emp = 'A'
    AND taxRegime IN ('2', '4')
    AND emp.tins_emp IN (1)
    /*and emp.codi_emp in (1510)*/
ORDER BY emp.codi_emp