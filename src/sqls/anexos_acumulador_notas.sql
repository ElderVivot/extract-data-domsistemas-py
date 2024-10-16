SELECT td_dados.codi_emp,
    list(td_dados.anexo) as anexos,
    max(td_dados.anexo_troca_fator_r) as anexo_troca_fator_r
FROM (
        select distinct aci.codi_emp,
            aci.SIMPLESN_ANEXO_IAC AS anexo,
            aci.SIMPLESN_TROCA_AUTOMATICA_DE_ANEXO_IAC as anexo_troca_fator_r
        from bethadba.EFACUMULADOR_VIGENCIA_IMPOSTOS as aci
            inner join bethadba.efsaidas as sai on sai.codi_emp = aci.codi_emp
            and sai.codi_acu = aci.codi_acu
        where aci.codi_emp = '#codi_emp#'
            and aci.vigencia_acu = (
                SELECT MAX(vig.vigencia_acu)
                FROM bethadba.EFACUMULADOR_VIGENCIA as vig
                WHERE vig.codi_emp = aci.codi_emp
                    and vig.codi_acu = aci.codi_acu
                    and vig.vigencia_acu <= sai.dsai_sai
            )
            and sai.dsai_sai BETWEEN date('#competence#') and date('#competence_fim#')
            and aci.codi_imp = 44
        UNION ALL
        select distinct aci.codi_emp,
            aci.SIMPLESN_ANEXO_IAC AS anexo,
            aci.SIMPLESN_TROCA_AUTOMATICA_DE_ANEXO_IAC as anexo_troca_fator_r
        from bethadba.EFACUMULADOR_VIGENCIA_IMPOSTOS as aci
            inner join bethadba.efservicos as ser on ser.codi_emp = aci.codi_emp
            and ser.codi_acu = aci.codi_acu
        where aci.codi_emp = '#codi_emp#'
            and aci.vigencia_acu = (
                SELECT MAX(vig.vigencia_acu)
                FROM bethadba.EFACUMULADOR_VIGENCIA as vig
                WHERE vig.codi_emp = aci.codi_emp
                    and vig.codi_acu = aci.codi_acu
                    and vig.vigencia_acu <= ser.dser_ser
            )
            and ser.dser_ser BETWEEN date('#competence#') and date('#competence_fim#')
            and aci.codi_imp = 44
    ) AS td_dados
GROUP BY td_dados.codi_emp