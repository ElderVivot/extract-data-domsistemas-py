SELECT
    /* quando for fazer filtros lembrar de criar um WHERE generico */
    TD_DADOS.codi_emp,
    nome_emp = td_dados.nome_emp,
    cgce = td_dados.cgce,
    mes = td_dados.mes,
    ano = td_dados.ano,
    total_saidas = SUM(TD_DADOS.VSAI),
    total_ipi = SUM(TD_DADOS.vipi),
    total_icms_substituicao = SUM(TD_DADOS.vst),
    total_servicos = SUM(TD_DADOS.vser),
    total_outros = SUM(TD_DADOS.vout),
    total_faturamento = total_saidas + total_servicos + total_outros - total_ipi - total_icms_substituicao,
    saldo_imposto = sum(td_dados.saldo_imposto),
    saldo_caixa = sum(td_dados.saldo_caixa),
    entradas = sum(td_dados.entradas),
    pis = sum(td_dados.pis),
    cofins = sum(td_dados.cofins),
    csll = sum(td_dados.csll),
    irpj = sum(td_dados.irpj),
    sn = sum(td_dados.sn),
    icms = sum(td_dados.icms),
    ipi = sum(td_dados.ipi)
FROM(
        SELECT SUM(TDAUX.VALOR_SAIDA) AS VSAI,
            geempre.codi_emp,
            geempre.nome_emp,
            SUM(COALESCE(TDEFIMPSAI_2_30.VALOR_IMPOSTO, 0)) AS VIPI,
            SUM(COALESCE(TD_IMP_9.VALOR_IMPOSTO, 0)) AS VST,
            0 AS VSER,
            0 AS VOUT,
            TDAUX.MES AS MES,
            TDAUX.ANO AS ANO,
            saldo_imposto = 0,
            saldo_caixa = 0,
            entradas = 0,
            pis = 0,
            cofins = 0,
            csll = 0,
            irpj = 0,
            sn = 0,
            icms = 0,
            ipi = 0,
            cgce = geempre.cgce_emp
        FROM BETHADBA.EFSAIDAS AS EFSAIDAS
            INNER JOIN BETHADBA.GEEMPRE AS GEEMPRE ON GEEMPRE.CODI_EMP = EFSAIDAS.CODI_EMP
            inner join bethadba.efparametro as par 
                  on    par.codi_emp = geempre.codi_emp
            INNER JOIN BETHADBA.EFPARAMETRO_VIGENCIA AS EFPARAMETRO_VIGENCIA ON EFPARAMETRO_VIGENCIA.CODI_EMP = EFSAIDAS.CODI_EMP
            INNER JOIN BETHADBA.EFACUMULADOR_VIGENCIA AS EFACUMULADOR_VIGENCIA ON EFACUMULADOR_VIGENCIA.CODI_EMP = EFSAIDAS.CODI_EMP
            AND EFACUMULADOR_VIGENCIA.CODI_ACU = EFSAIDAS.CODI_ACU,
            LATERAL(
                SELECT SUM(VLOR_ECA) AS VLOR_ECA
                FROM BETHADBA.EFTABECFM M
                    INNER JOIN BETHADBA.EFTABECFA A ON M.CODI_EMP = A.CODI_EMP
                    AND M.CODI_ECM = A.CODI_ECM
                    AND M.CODI_MEC = A.CODI_MEC
                    AND M.TIPO_ECM = A.TIPO_ECM
                WHERE M.CODI_EMP = EFSAIDAS.CODI_EMP
                    AND M.CODI_ECM = EFSAIDAS.CODI_SAI
                    AND M.TIPO_ECM = 'S'
                    AND M.DEDUZIR_ECM = 'S'
                    AND A.SITU_ECA IN ('DESC', 'CANC')
            ) AS TD_ECF,
            LATERAL(
                SELECT SUM(VLOR_ISA) AS VALOR_IMPOSTO
                FROM BETHADBA.EFIMPSAI AS EFIMPSAI
                WHERE EFIMPSAI.CODI_EMP = EFSAIDAS.CODI_EMP
                    AND EFIMPSAI.CODI_SAI = EFSAIDAS.CODI_SAI
                    AND EFIMPSAI.CODI_IMP IN (2, 30)
            ) AS TDEFIMPSAI_2_30,
            LATERAL(
                SELECT SUM(VLOR_ISA) AS VALOR_IMPOSTO
                FROM BETHADBA.EFIMPSAI AS EFIMPSAI
                    INNER JOIN BETHADBA.EFPARAMETRO_VIGENCIA AS EFPARAMETRO_VIGENCIA ON EFPARAMETRO_VIGENCIA.CODI_EMP = EFIMPSAI.CODI_EMP
                WHERE EFIMPSAI.CODI_EMP = EFSAIDAS.CODI_EMP
                    AND EFIMPSAI.CODI_SAI = EFSAIDAS.CODI_SAI
                    AND EFIMPSAI.CODI_IMP = 9
                    AND EFPARAMETRO_VIGENCIA.VIGENCIA_PAR = (
                        SELECT MAX(vig.vigencia_par)
                        FROM bethadba.EFPARAMETRO_VIGENCIA as vig
                        WHERE vig.codi_emp = efparametro_vigencia.codi_emp
                            and vig.vigencia_par <= efsaidas.dsai_sai
                    )
            ) AS TD_IMP_9,
            LATERAL(
                SELECT EFSAIDAS.VCON_SAI - COALESCE(TD_ECF.VLOR_ECA, 0) AS VALOR_SAIDA,
                    MONTH(EFSAIDAS.DSAI_SAI) AS MES,
                    YEAR(EFSAIDAS.DSAI_SAI) AS ANO
                FROM DSDBA.DUMMY
            ) AS TDAUX
        WHERE EFSAIDAS.codi_emp = '#codi_emp#'
            and EFSAIDAS.DSAI_SAI >= DATE('#competence#')
            AND EFSAIDAS.DSAI_SAI <= DATE('#competence_fim#')
            and ( EFSAIDAS.DSAI_SAI >= par.SIMPLESN_INICIO_SISTEMA_PAR OR par.SIMPLESN_INICIO_SISTEMA_PAR is null )
            AND EFACUMULADOR_VIGENCIA.IFAT_ACU = 'S'
            AND EFSAIDAS.CODI_NAT NOT IN (5933, 6933)
            AND EFSAIDAS.NOVO_ECF = 'N'
            AND (
                EFPARAMETRO_VIGENCIA.EMPRESA_REVENDA_VEICULOS = 'N'
                OR NOT (
                    EFPARAMETRO_VIGENCIA.EMPRESA_REVENDA_VEICULOS = 'S'
                    AND (
                        EFACUMULADOR_VIGENCIA.OPERACAO_VEICULOS_USADOS_ACU = 'S'
                        OR EFSAIDAS.CODI_NAT IN (5115, 6115)
                    )
                )
            )
            AND EFPARAMETRO_VIGENCIA.VIGENCIA_PAR = (
                SELECT MAX(vig.vigencia_par)
                FROM bethadba.EFPARAMETRO_VIGENCIA as vig
                WHERE vig.codi_emp = efparametro_vigencia.codi_emp
                    and vig.vigencia_par <= efsaidas.dsai_sai
            )
            AND EFACUMULADOR_VIGENCIA.VIGENCIA_ACU = (
                SELECT MAX(vig.VIGENCIA_ACU)
                FROM bethadba.EFACUMULADOR_VIGENCIA as vig
                WHERE vig.codi_emp = efsaidas.codi_emp
                    and vig.codi_acu = efsaidas.codi_acu
                    and vig.VIGENCIA_ACU <= efsaidas.dsai_sai
            )
        GROUP BY TDAUX.MES,
            geempre.codi_emp,
            geempre.nome_emp,
            TDAUX.ANO,
            cgce
        UNION ALL
        SELECT SUM(EFMVSPRO.VALOR_CONTABIL_MSP) AS VSAI,
            geempre.codi_emp,
            geempre.nome_emp,
            SUM(COALESCE(TDEFIMPSAI_2_30.VALOR_IMPOSTO, 0)) AS VIPI,
            SUM(COALESCE(TD_IMP_9.VALOR_IMPOSTO, 0)) AS VST,
            0 AS VSER,
            0 AS VOUT,
            TDAUX.MES AS MES,
            TDAUX.ANO AS ANO,
            saldo_imposto = 0,
            saldo_caixa = 0,
            entradas = 0,
            pis = 0,
            cofins = 0,
            csll = 0,
            irpj = 0,
            sn = 0,
            icms = 0,
            ipi = 0,
            cgce = geempre.cgce_emp
        FROM BETHADBA.EFSAIDAS AS EFSAIDAS
            INNER JOIN BETHADBA.GEEMPRE AS GEEMPRE ON GEEMPRE.CODI_EMP = EFSAIDAS.CODI_EMP
            inner join bethadba.efparametro as par 
                  on    par.codi_emp = geempre.codi_emp
            INNER JOIN BETHADBA.EFPARAMETRO_VIGENCIA AS EFPARAMETRO_VIGENCIA ON EFPARAMETRO_VIGENCIA.CODI_EMP = EFSAIDAS.CODI_EMP
            INNER JOIN BETHADBA.EFACUMULADOR_VIGENCIA AS EFACUMULADOR_VIGENCIA ON EFACUMULADOR_VIGENCIA.CODI_EMP = EFSAIDAS.CODI_EMP
            AND EFACUMULADOR_VIGENCIA.CODI_ACU = EFSAIDAS.CODI_ACU
            INNER JOIN BETHADBA.EFMVSPRO AS EFMVSPRO ON EFMVSPRO.CODI_EMP = EFSAIDAS.CODI_EMP
            AND EFMVSPRO.CODI_SAI = EFSAIDAS.CODI_SAI
            INNER JOIN BETHADBA.EFPRODUTOS AS EFPRODUTOS ON EFPRODUTOS.CODI_EMP = EFMVSPRO.CODI_EMP
            AND EFPRODUTOS.CODI_PDI = EFMVSPRO.CODI_PDI,
            LATERAL(
                SELECT SUM(VLOR_ECA) AS VLOR_ECA
                FROM BETHADBA.EFTABECFM M
                    INNER JOIN BETHADBA.EFTABECFA A ON M.CODI_EMP = A.CODI_EMP
                    AND M.CODI_ECM = A.CODI_ECM
                    AND M.CODI_MEC = A.CODI_MEC
                    AND M.TIPO_ECM = A.TIPO_ECM
                WHERE M.CODI_EMP = EFSAIDAS.CODI_EMP
                    AND M.CODI_ECM = EFSAIDAS.CODI_SAI
                    AND M.TIPO_ECM = 'S'
                    AND M.DEDUZIR_ECM = 'S'
                    AND A.SITU_ECA IN ('DESC', 'CANC')
            ) AS TD_ECF,
            LATERAL(
                SELECT SUM(VLOR_ISA) AS VALOR_IMPOSTO
                FROM BETHADBA.EFIMPSAI AS EFIMPSAI
                WHERE EFIMPSAI.CODI_EMP = EFSAIDAS.CODI_EMP
                    AND EFIMPSAI.CODI_SAI = EFSAIDAS.CODI_SAI
                    AND EFIMPSAI.CODI_IMP IN (2, 30)
            ) AS TDEFIMPSAI_2_30,
            LATERAL(
                SELECT SUM(VLOR_ISA) AS VALOR_IMPOSTO
                FROM BETHADBA.EFIMPSAI AS EFIMPSAI
                    INNER JOIN BETHADBA.EFPARAMETRO_VIGENCIA AS EFPARAMETRO_VIGENCIA ON EFPARAMETRO_VIGENCIA.CODI_EMP = EFIMPSAI.CODI_EMP
                WHERE EFIMPSAI.CODI_EMP = EFSAIDAS.CODI_EMP
                    AND EFIMPSAI.CODI_SAI = EFSAIDAS.CODI_SAI
                    AND EFIMPSAI.CODI_IMP = 9
                    AND EFPARAMETRO_VIGENCIA.VIGENCIA_PAR = (
                        SELECT MAX(vig.vigencia_par)
                        FROM bethadba.EFPARAMETRO_VIGENCIA as vig
                        WHERE vig.codi_emp = efparametro_vigencia.codi_emp
                            and vig.vigencia_par <= efsaidas.dsai_sai
                    )
            ) AS TD_IMP_9,
            LATERAL(
                SELECT EFSAIDAS.VCON_SAI - COALESCE(TD_ECF.VLOR_ECA, 0) AS VALOR_SAIDA,
                    MONTH(EFSAIDAS.DSAI_SAI) AS MES,
                    YEAR(EFSAIDAS.DSAI_SAI) AS ANO
                FROM DSDBA.DUMMY
            ) AS TDAUX,
            LATERAL(
                SELECT COALESCE(SUM(EFMVEPRO.VALOR_CONTABIL_MEP), 0) AS VALOR_CONTABIL
                FROM BETHADBA.EFMVEPRO AS EFMVEPRO
                WHERE EFMVEPRO.CODI_EMP = EFMVSPRO.CODI_EMP
                    AND EFMVEPRO.CODI_PDI = EFMVSPRO.CODI_PDI
                    AND EFMVEPRO.CODI_ENT = (
                        SELECT MAX(EFMVEPRO2.CODI_ENT)
                        FROM BETHADBA.EFMVEPRO AS EFMVEPRO2
                        WHERE EFMVEPRO2.CODI_EMP = EFMVEPRO.CODI_EMP
                            AND EFMVEPRO2.CODI_PDI = EFMVEPRO.CODI_PDI
                            AND EFMVEPRO2.CODI_SAI_DEVOLVIDA IS NULL
                            AND EFMVEPRO2.DATA_MEP <= EFSAIDAS.DSAI_SAI
                    )
            ) AS TD_ENTRADA
        WHERE EFSAIDAS.codi_emp = '#codi_emp#'
            AND EFSAIDAS.DSAI_SAI >= DATE('#competence#')
            AND EFSAIDAS.DSAI_SAI <= DATE('#competence_fim#')
            and ( EFSAIDAS.DSAI_SAI >= par.SIMPLESN_INICIO_SISTEMA_PAR OR par.SIMPLESN_INICIO_SISTEMA_PAR is null )
            AND EFACUMULADOR_VIGENCIA.IFAT_ACU = 'S'
            AND EFSAIDAS.CODI_NAT NOT IN (5933, 6933)
            AND EFSAIDAS.NOVO_ECF = 'N'
            AND EFPARAMETRO_VIGENCIA.EMPRESA_REVENDA_VEICULOS = 'S'
            AND (
                EFACUMULADOR_VIGENCIA.OPERACAO_VEICULOS_USADOS_ACU = 'S'
                OR EFSAIDAS.CODI_NAT IN (5115, 6115)
            )
            AND EFPARAMETRO_VIGENCIA.VIGENCIA_PAR = (
                SELECT MAX(vig.vigencia_par)
                FROM bethadba.EFPARAMETRO_VIGENCIA as vig
                WHERE vig.codi_emp = efparametro_vigencia.codi_emp
                    and vig.vigencia_par <= efsaidas.dsai_sai
            )
            AND EFACUMULADOR_VIGENCIA.VIGENCIA_ACU = (
                SELECT MAX(vig.VIGENCIA_ACU)
                FROM bethadba.EFACUMULADOR_VIGENCIA as vig
                WHERE vig.codi_emp = EFSAIDAS.codi_emp
                    and vig.codi_acu = EFSAIDAS.codi_acu
                    and vig.VIGENCIA_ACU <= EFSAIDAS.DSAI_SAI
            )
        GROUP BY TDAUX.MES,
            geempre.codi_emp,
            geempre.nome_emp,
            TDAUX.ANO,
            cgce
        UNION ALL
        SELECT SUM(SITUACAO.VALOR) AS VSAI,
            geempre.codi_emp,
            geempre.nome_emp,
            0 AS VIPI,
            0 AS VST,
            0 AS VSER,
            0 AS VOUT,
            TDAUX.MES AS MES,
            TDAUX.ANO AS ANO,
            saldo_imposto = 0,
            saldo_caixa = 0,
            entradas = 0,
            pis = 0,
            cofins = 0,
            csll = 0,
            irpj = 0,
            sn = 0,
            icms = 0,
            ipi = 0,
            cgce = geempre.cgce_emp
        FROM BETHADBA.EFECF_REDUCAO_Z AS REDUCAO_Z
            INNER JOIN BETHADBA.EFECF_REDUCAO_Z_SITUACAO_TRIBUTARIA_DETALHAMENTO AS SITUACAO ON SITUACAO.CODI_EMP = REDUCAO_Z.CODI_EMP
            AND SITUACAO.I_REDUCAO = REDUCAO_Z.I_REDUCAO
            INNER JOIN BETHADBA.GEEMPRE AS GEEMPRE ON GEEMPRE.CODI_EMP = REDUCAO_Z.CODI_EMP
            inner join bethadba.efparametro as par 
                  on    par.codi_emp = geempre.codi_emp
            INNER JOIN BETHADBA.EFECF_REDUCAO_Z_SITUACAO_TRIBUTARIA AS EFECF_REDUCAO_Z_SITUACAO_TRIBUTARIA ON EFECF_REDUCAO_Z_SITUACAO_TRIBUTARIA.CODI_EMP = SITUACAO.CODI_EMP
            AND EFECF_REDUCAO_Z_SITUACAO_TRIBUTARIA.I_REDUCAO = SITUACAO.I_REDUCAO
            AND EFECF_REDUCAO_Z_SITUACAO_TRIBUTARIA.I_SITUACAO = SITUACAO.I_SITUACAO,
            LATERAL(
                SELECT MONTH(REDUCAO_Z.DATA_REDUCAO) AS MES,
                    YEAR(REDUCAO_Z.DATA_REDUCAO) AS ANO
                FROM DSDBA.DUMMY
            ) AS TDAUX
        WHERE REDUCAO_Z.codi_emp = '#codi_emp#'
            and REDUCAO_Z.DATA_REDUCAO >= DATE('#competence#')
            AND REDUCAO_Z.DATA_REDUCAO <= DATE('#competence_fim#')
            and ( REDUCAO_Z.DATA_REDUCAO >= par.SIMPLESN_INICIO_SISTEMA_PAR OR par.SIMPLESN_INICIO_SISTEMA_PAR is null )
            AND EFECF_REDUCAO_Z_SITUACAO_TRIBUTARIA.OPERACAO IN (1, 2, 3, 7)
        GROUP BY TDAUX.MES,
            geempre.codi_emp,
            geempre.nome_emp,
            TDAUX.ANO,
            cgce
        UNION ALL
        SELECT 0 AS VSAI,
            geempre.codi_emp,
            geempre.nome_emp,
            0 AS VIPI,
            0 AS VST,
            SUM(SITUACAO.VALOR) AS VSER,
            0 AS VOUT,
            TDAUX.MES AS MES,
            TDAUX.ANO AS ANO,
            saldo_imposto = 0,
            saldo_caixa = 0,
            entradas = 0,
            pis = 0,
            cofins = 0,
            csll = 0,
            irpj = 0,
            sn = 0,
            icms = 0,
            ipi = 0,
            cgce = geempre.cgce_emp
        FROM BETHADBA.EFECF_REDUCAO_Z AS REDUCAO_Z
            INNER JOIN BETHADBA.EFECF_REDUCAO_Z_SITUACAO_TRIBUTARIA_DETALHAMENTO AS SITUACAO ON SITUACAO.CODI_EMP = REDUCAO_Z.CODI_EMP
            AND SITUACAO.I_REDUCAO = REDUCAO_Z.I_REDUCAO
            INNER JOIN BETHADBA.GEEMPRE AS GEEMPRE ON GEEMPRE.CODI_EMP = REDUCAO_Z.CODI_EMP
            inner join bethadba.efparametro as par 
                  on    par.codi_emp = geempre.codi_emp
            INNER JOIN BETHADBA.EFECF_REDUCAO_Z_SITUACAO_TRIBUTARIA AS EFECF_REDUCAO_Z_SITUACAO_TRIBUTARIA ON EFECF_REDUCAO_Z_SITUACAO_TRIBUTARIA.CODI_EMP = SITUACAO.CODI_EMP
            AND EFECF_REDUCAO_Z_SITUACAO_TRIBUTARIA.I_REDUCAO = SITUACAO.I_REDUCAO
            AND EFECF_REDUCAO_Z_SITUACAO_TRIBUTARIA.I_SITUACAO = SITUACAO.I_SITUACAO,
            LATERAL(
                SELECT MONTH(REDUCAO_Z.DATA_REDUCAO) AS MES,
                    YEAR(REDUCAO_Z.DATA_REDUCAO) AS ANO
                FROM DSDBA.DUMMY
            ) AS TDAUX
        WHERE REDUCAO_Z.codi_emp = '#codi_emp#'
            and REDUCAO_Z.DATA_REDUCAO >= DATE('#competence#')
            AND REDUCAO_Z.DATA_REDUCAO <= DATE('#competence_fim#')
            and ( REDUCAO_Z.DATA_REDUCAO >= par.SIMPLESN_INICIO_SISTEMA_PAR OR par.SIMPLESN_INICIO_SISTEMA_PAR is null )
            AND EFECF_REDUCAO_Z_SITUACAO_TRIBUTARIA.OPERACAO IN (8, 9, 10)
        GROUP BY TDAUX.MES,
            geempre.codi_emp,
            geempre.nome_emp,
            TDAUX.ANO,
            cgce
        UNION ALL
        SELECT SUM(REDUCAO_Z_BILHETE.VENDA_LIQUIDA) AS VSAI,
            geempre.codi_emp,
            geempre.nome_emp,
            0 AS VIPI,
            0 AS VST,
            0 AS VSER,
            0 AS VOUT,
            TDAUX.MES AS MES,
            TDAUX.ANO AS ANO,
            saldo_imposto = 0,
            saldo_caixa = 0,
            entradas = 0,
            pis = 0,
            cofins = 0,
            csll = 0,
            irpj = 0,
            sn = 0,
            icms = 0,
            ipi = 0,
            cgce = geempre.cgce_emp
        FROM BETHADBA.EFECF_REDUCAO_Z_BILHETE AS REDUCAO_Z_BILHETE
            INNER JOIN BETHADBA.GEEMPRE AS GEEMPRE ON GEEMPRE.CODI_EMP = REDUCAO_Z_BILHETE.CODI_EMP
            inner join bethadba.efparametro as par 
                  on    par.codi_emp = geempre.codi_emp,
            LATERAL(
                SELECT MONTH(REDUCAO_Z_BILHETE.DATA_REDUCAO) AS MES,
                    YEAR(REDUCAO_Z_BILHETE.DATA_REDUCAO) AS ANO
                FROM DSDBA.DUMMY
            ) AS TDAUX
        WHERE REDUCAO_Z_BILHETE.codi_emp = '#codi_emp#'
            and REDUCAO_Z_BILHETE.DATA_REDUCAO >= DATE('#competence#')
            AND REDUCAO_Z_BILHETE.DATA_REDUCAO <= DATE('#competence_fim#')
            and ( REDUCAO_Z_BILHETE.DATA_REDUCAO >= par.SIMPLESN_INICIO_SISTEMA_PAR OR par.SIMPLESN_INICIO_SISTEMA_PAR is null )
        GROUP BY TDAUX.MES,
            geempre.codi_emp,
            geempre.nome_emp,
            TDAUX.ANO,
            cgce
        UNION ALL
        SELECT SUM(RESUMO_BILHETE.VALOR_TOTAL) AS VSAI,
            geempre.codi_emp,
            geempre.nome_emp,
            0 AS VIPI,
            0 AS VST,
            0 AS VSER,
            0 AS VOUT,
            TDAUX.MES AS MES,
            TDAUX.ANO AS ANO,
            saldo_imposto = 0,
            saldo_caixa = 0,
            entradas = 0,
            pis = 0,
            cofins = 0,
            csll = 0,
            irpj = 0,
            sn = 0,
            icms = 0,
            ipi = 0,
            cgce = geempre.cgce_emp
        FROM BETHADBA.EFRESUMO_MOVIMENTO_DIARIO AS RESUMO
            INNER JOIN BETHADBA.EFRESUMO_MOVIMENTO_DIARIO_BILHETE AS RESUMO_BILHETE ON RESUMO.CODI_EMP = RESUMO_BILHETE.CODI_EMP
            AND RESUMO.I_RESUMO = RESUMO_BILHETE.I_RESUMO
            INNER JOIN BETHADBA.GEEMPRE AS GEEMPRE ON GEEMPRE.CODI_EMP = RESUMO_BILHETE.CODI_EMP
            inner join bethadba.efparametro as par 
                  on    par.codi_emp = geempre.codi_emp
            INNER JOIN BETHADBA.EFACUMULADOR_VIGENCIA AS EFACUMULADOR_VIGENCIA ON EFACUMULADOR_VIGENCIA.CODI_EMP = RESUMO_BILHETE.CODI_EMP
            AND EFACUMULADOR_VIGENCIA.CODI_ACU = RESUMO_BILHETE.CODI_ACU,
            LATERAL(
                SELECT MONTH(RESUMO.DATA_EMISSAO) AS MES,
                    YEAR(RESUMO.DATA_EMISSAO) AS ANO
                FROM DSDBA.DUMMY
            ) AS TDAUX
        WHERE RESUMO.codi_emp = '#codi_emp#'
            and RESUMO.DATA_EMISSAO >= DATE('#competence#')
            AND RESUMO.DATA_EMISSAO <= DATE('#competence_fim#')
            and ( RESUMO.DATA_EMISSAO >= par.SIMPLESN_INICIO_SISTEMA_PAR OR par.SIMPLESN_INICIO_SISTEMA_PAR is null )
            AND EFACUMULADOR_VIGENCIA.IFAT_ACU = 'S'
            AND RESUMO_BILHETE.CODI_NAT NOT IN(5933, 6933)
            AND EFACUMULADOR_VIGENCIA.VIGENCIA_ACU = (
                SELECT MAX(vig.VIGENCIA_ACU)
                FROM bethadba.EFACUMULADOR_VIGENCIA as vig
                WHERE vig.codi_emp = RESUMO_BILHETE.codi_emp
                    and vig.codi_acu = RESUMO_BILHETE.codi_acu
                    and vig.VIGENCIA_ACU <= RESUMO.DATA_EMISSAO
            )
        GROUP BY TDAUX.MES,
            geempre.codi_emp,
            geempre.nome_emp,
            TDAUX.ANO,
            cgce
        UNION ALL
        SELECT SUM(BILHETE.VALOR_TOTAL) AS VSAI,
            geempre.codi_emp,
            geempre.nome_emp,
            0 AS VIPI,
            0 AS VST,
            0 AS VSER,
            0 AS VOUT,
            TDAUX.MES AS MES,
            TDAUX.ANO AS ANO,
            saldo_imposto = 0,
            saldo_caixa = 0,
            entradas = 0,
            pis = 0,
            cofins = 0,
            csll = 0,
            irpj = 0,
            sn = 0,
            icms = 0,
            ipi = 0,
            cgce = geempre.cgce_emp
        FROM BETHADBA.EFBILHETE_PASSAGEM AS BILHETE
            INNER JOIN BETHADBA.GEEMPRE AS GEEMPRE ON GEEMPRE.CODI_EMP = BILHETE.CODI_EMP
            inner join bethadba.efparametro as par 
                  on    par.codi_emp = geempre.codi_emp
            INNER JOIN BETHADBA.EFACUMULADOR_VIGENCIA AS EFACUMULADOR_VIGENCIA ON EFACUMULADOR_VIGENCIA.CODI_EMP = BILHETE.CODI_EMP
            AND EFACUMULADOR_VIGENCIA.CODI_ACU = BILHETE.CODI_ACU,
            LATERAL(
                SELECT MONTH(BILHETE.DATA_EMISSAO) AS MES,
                    YEAR(BILHETE.DATA_EMISSAO) AS ANO
                FROM DSDBA.DUMMY
            ) AS TDAUX
        WHERE BILHETE.codi_emp = '#codi_emp#'
            and BILHETE.DATA_EMISSAO >= DATE('#competence#')
            AND BILHETE.DATA_EMISSAO <= DATE('#competence_fim#')
            and ( BILHETE.DATA_EMISSAO >= par.SIMPLESN_INICIO_SISTEMA_PAR OR par.SIMPLESN_INICIO_SISTEMA_PAR is null )
            AND EFACUMULADOR_VIGENCIA.IFAT_ACU = 'S'
            AND BILHETE.CODI_NAT NOT IN(5933, 6933)
            AND EFACUMULADOR_VIGENCIA.VIGENCIA_ACU = (
                SELECT MAX(vig.VIGENCIA_ACU)
                FROM bethadba.EFACUMULADOR_VIGENCIA as vig
                WHERE vig.codi_emp = BILHETE.codi_emp
                    and vig.codi_acu = BILHETE.codi_acu
                    and vig.VIGENCIA_ACU <= BILHETE.DATA_EMISSAO
            )
        GROUP BY TDAUX.MES,
            geempre.codi_emp,
            geempre.nome_emp,
            TDAUX.ANO,
            cgce
        UNION ALL
        SELECT 0 AS VSAI,
            geempre.codi_emp,
            geempre.nome_emp,
            0 AS VIPI,
            0 AS VST,
            SUM(TDAUX.VSER) AS VSER,
            0 AS VOUT,
            TDAUX.MES AS MES,
            TDAUX.ANO AS ANO,
            saldo_imposto = 0,
            saldo_caixa = 0,
            entradas = 0,
            pis = 0,
            cofins = 0,
            csll = 0,
            irpj = 0,
            sn = 0,
            icms = 0,
            ipi = 0,
            cgce = geempre.cgce_emp
        FROM BETHADBA.EFSERVICOS AS EFSERVICOS
            INNER JOIN BETHADBA.EFACUMULADOR_VIGENCIA AS EFACUMULADOR_VIGENCIA ON EFACUMULADOR_VIGENCIA.CODI_EMP = EFSERVICOS.CODI_EMP
            AND EFACUMULADOR_VIGENCIA.CODI_ACU = EFSERVICOS.CODI_ACU
            INNER JOIN BETHADBA.GEEMPRE AS GEEMPRE ON GEEMPRE.CODI_EMP = EFSERVICOS.CODI_EMP
            inner join bethadba.efparametro as par 
                  on    par.codi_emp = geempre.codi_emp,
            LATERAL(
                SELECT SUM(A.VLOR_ECA) AS VALOR_CANCELAMENT_DESCONTO
                FROM BETHADBA.EFTABECFM M,
                    BETHADBA.EFTABECFA A
                WHERE M.TIPO_ECM = 'V'
                    AND M.DEDUZIR_ECM = 'S'
                    AND M.CODI_EMP = A.CODI_EMP
                    AND M.CODI_ECM = A.CODI_ECM
                    AND M.CODI_MEC = A.CODI_MEC
                    AND M.TIPO_ECM = A.TIPO_ECM
                    AND A.SITU_ECA IN ('DESC', 'CANC')
                    AND M.CODI_EMP = EFSERVICOS.CODI_EMP
                    AND M.CODI_ECM = EFSERVICOS.CODI_SER
            ) AS TDCANCELAMENTO_DESCONTO,
            LATERAL(
                SELECT CASE
                        WHEN EFSERVICOS.VCON_SER - COALESCE(
                            TDCANCELAMENTO_DESCONTO.VALOR_CANCELAMENT_DESCONTO,
                            0
                        ) > 0 THEN EFSERVICOS.VCON_SER - COALESCE(
                            TDCANCELAMENTO_DESCONTO.VALOR_CANCELAMENT_DESCONTO,
                            0
                        )
                        ELSE 0
                    END AS VSER,
                    MONTH(EFSERVICOS.DSER_SER) AS MES,
                    YEAR(EFSERVICOS.DSER_SER) AS ANO
                FROM DSDBA.DUMMY
            ) AS TDAUX
        WHERE EFSERVICOS.codi_emp = '#codi_emp#'
            and EFSERVICOS.DSER_SER >= DATE('#competence#')
            AND EFSERVICOS.DSER_SER <= DATE('#competence_fim#')
            and ( EFSERVICOS.DSER_SER >= par.SIMPLESN_INICIO_SISTEMA_PAR OR par.SIMPLESN_INICIO_SISTEMA_PAR is null )
            AND EFACUMULADOR_VIGENCIA.IFAT_ACU = 'S'
            AND EFACUMULADOR_VIGENCIA.VIGENCIA_ACU = (
                SELECT MAX(vig.VIGENCIA_ACU)
                FROM bethadba.EFACUMULADOR_VIGENCIA as vig
                WHERE vig.codi_emp = EFSERVICOS.codi_emp
                    and vig.codi_acu = EFSERVICOS.codi_acu
                    and vig.VIGENCIA_ACU <= EFSERVICOS.DSER_SER
            )
        GROUP BY TDAUX.MES,
            geempre.codi_emp,
            geempre.nome_emp,
            TDAUX.ANO,
            cgce
        UNION ALL
        SELECT 0 AS VSAI,
            geempre.codi_emp,
            geempre.nome_emp,
            0 AS VIPI,
            0 AS VST,
            0 AS VSER,
            SUM(EFMOVACU.VLOR_MAC) AS VOUT,
            TDAUX.MES AS MES,
            TDAUX.ANO AS ANO,
            saldo_imposto = 0,
            saldo_caixa = 0,
            entradas = 0,
            pis = 0,
            cofins = 0,
            csll = 0,
            irpj = 0,
            sn = 0,
            icms = 0,
            ipi = 0,
            cgce = geempre.cgce_emp
        FROM BETHADBA.EFMOVACU AS EFMOVACU
            INNER JOIN BETHADBA.EFACUMULADOR_VIGENCIA AS EFACUMULADOR_VIGENCIA ON EFACUMULADOR_VIGENCIA.CODI_EMP = EFMOVACU.CODI_EMP
            AND EFACUMULADOR_VIGENCIA.CODI_ACU = EFMOVACU.CODI_ACU
            INNER JOIN BETHADBA.GEEMPRE AS GEEMPRE ON GEEMPRE.CODI_EMP = EFMOVACU.CODI_EMP
            inner join bethadba.efparametro as par 
                  on    par.codi_emp = geempre.codi_emp,
            LATERAL(
                SELECT MONTH(EFMOVACU.DATA_MAC) AS MES,
                    YEAR(EFMOVACU.DATA_MAC) AS ANO
                FROM DSDBA.DUMMY
            ) AS TDAUX
        WHERE EFMOVACU.codi_emp = '#codi_emp#'
            and EFMOVACU.DATA_MAC >= DATE('#competence#')
            AND EFMOVACU.DATA_MAC <= DATE('#competence_fim#')
            and ( EFMOVACU.DATA_MAC >= par.SIMPLESN_INICIO_SISTEMA_PAR OR par.SIMPLESN_INICIO_SISTEMA_PAR is null )
            AND EFMOVACU.OPER_MAC IN (1, 2, 6, 9)
            AND EFACUMULADOR_VIGENCIA.IFAT_ACU = 'S'
            AND EFACUMULADOR_VIGENCIA.VIGENCIA_ACU = (
                SELECT MAX(vig.VIGENCIA_ACU)
                FROM bethadba.EFACUMULADOR_VIGENCIA as vig
                WHERE vig.codi_emp = EFMOVACU.codi_emp
                    and vig.codi_acu = EFMOVACU.codi_acu
                    and vig.VIGENCIA_ACU <= EFMOVACU.DATA_MAC
            )
        GROUP BY TDAUX.MES,
            geempre.codi_emp,
            geempre.nome_emp,
            TDAUX.ANO,
            cgce
        UNION ALL
        SELECT - SUM(EFENTRADAS.VCON_ENT) AS VSAI,
            geempre.codi_emp,
            geempre.nome_emp,
            - SUM(COALESCE(TDIMPOSTO_PIS.VALOR_IMPOSTO, 0)) AS VIPI,
            - SUM(COALESCE(TDIMPOSTO_9.VALOR_IMPOSTO, 0)) AS VST,
            0 AS VSER,
            0 AS VOUT,
            TDAUX.MES AS MES,
            TDAUX.ANO AS ANO,
            saldo_imposto = 0,
            saldo_caixa = 0,
            entradas = 0,
            pis = 0,
            cofins = 0,
            csll = 0,
            irpj = 0,
            sn = 0,
            icms = 0,
            ipi = 0,
            cgce = geempre.cgce_emp
        FROM BETHADBA.EFENTRADAS AS EFENTRADAS
            INNER JOIN BETHADBA.EFACUMULADOR_VIGENCIA AS EFACUMULADOR_VIGENCIA ON EFACUMULADOR_VIGENCIA.CODI_EMP = EFENTRADAS.CODI_EMP
            AND EFACUMULADOR_VIGENCIA.CODI_ACU = EFENTRADAS.CODI_ACU
            INNER JOIN BETHADBA.GEEMPRE AS GEEMPRE ON GEEMPRE.CODI_EMP = EFENTRADAS.CODI_EMP
            inner join bethadba.efparametro as par 
                  on    par.codi_emp = geempre.codi_emp,
            LATERAL(
                SELECT SUM(EFIMPENT.VLOR_IEN) AS VALOR_IMPOSTO
                FROM BETHADBA.EFIMPENT AS EFIMPENT
                WHERE EFIMPENT.CODI_EMP = EFENTRADAS.CODI_EMP
                    AND EFIMPENT.CODI_ENT = EFENTRADAS.CODI_ENT
                    AND EFIMPENT.CODI_IMP IN (2, 30)
            ) TDIMPOSTO_PIS,
            LATERAL(
                SELECT SUM(IMPOSTO_9.VLOR_IEN) AS VALOR_IMPOSTO
                FROM BETHADBA.EFIMPENT AS IMPOSTO_9
                WHERE IMPOSTO_9.CODI_EMP = EFENTRADAS.CODI_EMP
                    AND IMPOSTO_9.CODI_ENT = EFENTRADAS.CODI_ENT
                    AND IMPOSTO_9.CODI_IMP = 9
            ) AS TDIMPOSTO_9,
            LATERAL(
                SELECT MONTH(EFENTRADAS.DENT_ENT) AS MES,
                    YEAR(EFENTRADAS.DENT_ENT) AS ANO
                FROM DSDBA.DUMMY
            ) AS TDAUX
        WHERE EFENTRADAS.codi_emp = '#codi_emp#'
            and EFENTRADAS.DENT_ENT >= DATE('#competence#')
            AND EFENTRADAS.DENT_ENT <= DATE('#competence_fim#')
            and ( EFENTRADAS.DENT_ENT >= par.SIMPLESN_INICIO_SISTEMA_PAR OR par.SIMPLESN_INICIO_SISTEMA_PAR is null )
            AND EFENTRADAS.CODI_NAT NOT IN (1933, 2933)
            AND EFACUMULADOR_VIGENCIA.IFAT_ACU = 'S'
            AND EFACUMULADOR_VIGENCIA.IDEV_ACU = 'S'
            AND EFACUMULADOR_VIGENCIA.VIGENCIA_ACU = (
                SELECT MAX(vig.VIGENCIA_ACU)
                FROM bethadba.EFACUMULADOR_VIGENCIA as vig
                WHERE vig.codi_emp = EFENTRADAS.codi_emp
                    and vig.codi_acu = EFENTRADAS.codi_acu
                    and vig.VIGENCIA_ACU <= EFENTRADAS.DENT_ENT
            )
        GROUP BY TDAUX.MES,
            geempre.codi_emp,
            geempre.nome_emp,
            TDAUX.ANO,
            cgce
        UNION ALL
        SELECT 0 AS VSAI,
            geempre.codi_emp,
            geempre.nome_emp,
            0 AS VIPI,
            0 AS VST,
            0 AS VSER,
            - SUM(EFMOVACU.VLOR_MAC) AS VOUT,
            TDAUX.MES AS MES,
            TDAUX.ANO AS ANO,
            saldo_imposto = 0,
            saldo_caixa = 0,
            entradas = 0,
            pis = 0,
            cofins = 0,
            csll = 0,
            irpj = 0,
            sn = 0,
            icms = 0,
            ipi = 0,
            cgce = geempre.cgce_emp
        FROM BETHADBA.EFMOVACU AS EFMOVACU
            INNER JOIN BETHADBA.EFACUMULADOR_VIGENCIA AS EFACUMULADOR_VIGENCIA ON EFACUMULADOR_VIGENCIA.CODI_EMP = EFMOVACU.CODI_EMP
            AND EFACUMULADOR_VIGENCIA.CODI_ACU = EFMOVACU.CODI_ACU
            INNER JOIN BETHADBA.GEEMPRE AS GEEMPRE ON GEEMPRE.CODI_EMP = EFMOVACU.CODI_EMP
            inner join bethadba.efparametro as par 
                  on    par.codi_emp = geempre.codi_emp,
            LATERAL(
                SELECT MONTH(EFMOVACU.DATA_MAC) AS MES,
                    YEAR(EFMOVACU.DATA_MAC) AS ANO
                FROM DSDBA.DUMMY
            ) AS TDAUX
        WHERE EFMOVACU.codi_emp = '#codi_emp#'
            and EFMOVACU.DATA_MAC >= DATE('#competence#')
            AND EFMOVACU.DATA_MAC <= DATE('#competence_fim#')
            and ( EFMOVACU.DATA_MAC >= par.SIMPLESN_INICIO_SISTEMA_PAR OR par.SIMPLESN_INICIO_SISTEMA_PAR is null )
            AND EFMOVACU.OPER_MAC IN (3, 4, 5)
            AND EFACUMULADOR_VIGENCIA.IFAT_ACU = 'S'
            AND EFACUMULADOR_VIGENCIA.VIGENCIA_ACU = (
                SELECT MAX(vig.VIGENCIA_ACU)
                FROM bethadba.EFACUMULADOR_VIGENCIA as vig
                WHERE vig.codi_emp = EFMOVACU.codi_emp
                    and vig.codi_acu = EFMOVACU.codi_acu
                    and vig.VIGENCIA_ACU <= EFMOVACU.DATA_MAC
            )
        GROUP BY TDAUX.MES,
            geempre.codi_emp,
            geempre.nome_emp,
            TDAUX.ANO,
            cgce
        UNION ALL
        SELECT 0 AS VSAI,
            geempre.codi_emp,
            geempre.nome_emp,
            SUM(COALESCE(TDIMPOSTO_IPI.VALOR_IMPOSTO, 0)) AS VIPI,
            0 AS VST,
            SUM(TDAUX.VSER) AS VSER,
            0 AS VOUT,
            TDAUX.MES AS MES,
            TDAUX.ANO AS ANO,
            saldo_imposto = 0,
            saldo_caixa = 0,
            entradas = 0,
            pis = 0,
            cofins = 0,
            csll = 0,
            irpj = 0,
            sn = 0,
            icms = 0,
            ipi = 0,
            cgce = geempre.cgce_emp
        FROM BETHADBA.EFSAIDAS AS EFSAIDAS
            INNER JOIN BETHADBA.EFACUMULADOR_VIGENCIA AS EFACUMULADOR_VIGENCIA ON EFACUMULADOR_VIGENCIA.CODI_EMP = EFSAIDAS.CODI_EMP
            AND EFACUMULADOR_VIGENCIA.CODI_ACU = EFSAIDAS.CODI_ACU
            INNER JOIN BETHADBA.GEEMPRE AS GEEMPRE ON GEEMPRE.CODI_EMP = EFSAIDAS.CODI_EMP
            inner join bethadba.efparametro as par 
                  on    par.codi_emp = geempre.codi_emp,
            LATERAL(
                SELECT SUM(EFIMPSAI.VLOR_ISA) AS VALOR_IMPOSTO
                FROM BETHADBA.EFIMPSAI AS EFIMPSAI
                WHERE EFIMPSAI.CODI_EMP = EFSAIDAS.CODI_EMP
                    AND EFIMPSAI.CODI_SAI = EFSAIDAS.CODI_SAI
                    AND EFIMPSAI.CODI_IMP = 2
            ) AS TDIMPOSTO_IPI,
            LATERAL(
                SELECT SUM(A.VLOR_ECA) AS VALOR_DESCONTO
                FROM BETHADBA.EFTABECFM M,
                    BETHADBA.EFTABECFA A
                WHERE M.TIPO_ECM = 'S'
                    AND M.DEDUZIR_ECM = 'S'
                    AND M.CODI_EMP = A.CODI_EMP
                    AND M.CODI_ECM = A.CODI_ECM
                    AND M.CODI_MEC = A.CODI_MEC
                    AND M.TIPO_ECM = A.TIPO_ECM
                    AND A.SITU_ECA IN ('DESC', 'CANC')
                    AND M.CODI_EMP = EFSAIDAS.CODI_EMP
                    AND M.CODI_ECM = EFSAIDAS.CODI_SAI
            ) AS TDCANCELAMENTO_DESCONTO,
            LATERAL(
                SELECT CASE
                        WHEN EFSAIDAS.VCON_SAI - COALESCE(TDCANCELAMENTO_DESCONTO.VALOR_DESCONTO, 0) > 0 THEN EFSAIDAS.VCON_SAI - COALESCE(TDCANCELAMENTO_DESCONTO.VALOR_DESCONTO, 0)
                        ELSE 0
                    END AS VSER,
                    MONTH(EFSAIDAS.DSAI_SAI) AS MES,
                    YEAR(EFSAIDAS.DSAI_SAI) AS ANO
                FROM DSDBA.DUMMY
            ) AS TDAUX
        WHERE EFSAIDAS.codi_emp = '#codi_emp#'
            and EFSAIDAS.DSAI_SAI >= DATE('#competence#')
            AND EFSAIDAS.DSAI_SAI <= DATE('#competence_fim#')
            and ( EFSAIDAS.DSAI_SAI >= par.SIMPLESN_INICIO_SISTEMA_PAR OR par.SIMPLESN_INICIO_SISTEMA_PAR is null )
            AND EFSAIDAS.CODI_NAT IN (5933, 6933)
            AND EFSAIDAS.NOVO_ECF = 'N'
            AND EFACUMULADOR_VIGENCIA.IFAT_ACU = 'S'
            AND EFACUMULADOR_VIGENCIA.VIGENCIA_ACU = (
                SELECT MAX(vig.VIGENCIA_ACU)
                FROM bethadba.EFACUMULADOR_VIGENCIA as vig
                WHERE vig.codi_emp = EFSAIDAS.codi_emp
                    and vig.codi_acu = EFSAIDAS.codi_acu
                    and vig.VIGENCIA_ACU <= EFSAIDAS.DSAI_SAI
            )
        GROUP BY TDAUX.MES,
            geempre.codi_emp,
            geempre.nome_emp,
            TDAUX.ANO,
            cgce
        UNION ALL
        SELECT 0 AS VSAI,
            geempre.codi_emp,
            geempre.nome_emp,
            - SUM(COALESCE(TDIMPOSTO_PIS.VALOR_IMPOSTO, 0)) AS VIPI,
            0 AS VST,
            - SUM(EFENTRADAS.VCON_ENT) AS VSER,
            0 AS VOUT,
            TDAUX.MES AS MES,
            TDAUX.ANO AS ANO,
            saldo_imposto = 0,
            saldo_caixa = 0,
            entradas = 0,
            pis = 0,
            cofins = 0,
            csll = 0,
            irpj = 0,
            sn = 0,
            icms = 0,
            ipi = 0,
            cgce = geempre.cgce_emp
        FROM BETHADBA.EFENTRADAS AS EFENTRADAS
            INNER JOIN BETHADBA.EFACUMULADOR_VIGENCIA AS EFACUMULADOR_VIGENCIA ON EFACUMULADOR_VIGENCIA.CODI_EMP = EFENTRADAS.CODI_EMP
            AND EFACUMULADOR_VIGENCIA.CODI_ACU = EFENTRADAS.CODI_ACU
            INNER JOIN BETHADBA.GEEMPRE AS GEEMPRE ON GEEMPRE.CODI_EMP = EFENTRADAS.CODI_EMP
            inner join bethadba.efparametro as par 
                  on    par.codi_emp = geempre.codi_emp,
            LATERAL(
                SELECT SUM(EFIMPENT.VLOR_IEN) AS VALOR_IMPOSTO
                FROM BETHADBA.EFIMPENT AS EFIMPENT
                WHERE EFIMPENT.CODI_EMP = EFENTRADAS.CODI_EMP
                    AND EFIMPENT.CODI_ENT = EFENTRADAS.CODI_ENT
                    AND EFIMPENT.CODI_IMP IN (2, 30)
            ) AS TDIMPOSTO_PIS,
            LATERAL(
                SELECT MONTH(EFENTRADAS.DENT_ENT) AS MES,
                    YEAR(EFENTRADAS.DENT_ENT) AS ANO
                FROM DSDBA.DUMMY
            ) AS TDAUX
        WHERE EFENTRADAS.codi_emp = '#codi_emp#'
            and EFENTRADAS.DENT_ENT >= DATE('#competence#')
            AND EFENTRADAS.DENT_ENT <= DATE('#competence_fim#')
            and ( EFENTRADAS.DENT_ENT >= par.SIMPLESN_INICIO_SISTEMA_PAR OR par.SIMPLESN_INICIO_SISTEMA_PAR is null )
            AND EFENTRADAS.CODI_NAT IN (1933, 2933)
            AND EFACUMULADOR_VIGENCIA.IFAT_ACU = 'S'
            AND EFACUMULADOR_VIGENCIA.IDEV_ACU = 'S'
            AND EFACUMULADOR_VIGENCIA.VIGENCIA_ACU = (
                SELECT MAX(vig.VIGENCIA_ACU)
                FROM bethadba.EFACUMULADOR_VIGENCIA as vig
                WHERE vig.codi_emp = EFENTRADAS.codi_emp
                    and vig.codi_acu = EFENTRADAS.codi_acu
                    and vig.VIGENCIA_ACU <= EFENTRADAS.dent_ent
            )
        GROUP BY TDAUX.MES,
            geempre.codi_emp,
            geempre.nome_emp,
            TDAUX.ANO,
            cgce
        UNION ALL
        SELECT 0 AS VSAI,
            geempre.codi_emp,
            geempre.nome_emp,
            SUM(COALESCE(TDIMPOSTO_IPI.VALOR_IMPOSTO, 0)) AS VIPI,
            0 AS VST,
            0 AS VSER,
            0 AS VOUT,
            TDAUX.MES AS MES,
            TDAUX.ANO AS ANO,
            saldo_imposto = 0,
            saldo_caixa = 0,
            entradas = 0,
            pis = 0,
            cofins = 0,
            csll = 0,
            irpj = 0,
            sn = 0,
            icms = 0,
            ipi = 0,
            cgce = geempre.cgce_emp
        FROM BETHADBA.EFSAIDAS AS EFSAIDAS
            INNER JOIN BETHADBA.EFACUMULADOR_VIGENCIA AS EFACUMULADOR_VIGENCIA ON EFACUMULADOR_VIGENCIA.CODI_EMP = EFSAIDAS.CODI_EMP
            AND EFACUMULADOR_VIGENCIA.CODI_ACU = EFSAIDAS.CODI_ACU
            INNER JOIN BETHADBA.GEEMPRE AS GEEMPRE ON GEEMPRE.CODI_EMP = EFSAIDAS.CODI_EMP
            inner join bethadba.efparametro as par 
                  on    par.codi_emp = geempre.codi_emp,
            LATERAL(
                SELECT SUM(EFIMPSAI.VLOR_ISA) AS VALOR_IMPOSTO
                FROM BETHADBA.EFIMPSAI AS EFIMPSAI
                WHERE EFIMPSAI.CODI_EMP = EFSAIDAS.CODI_EMP
                    AND EFIMPSAI.CODI_SAI = EFSAIDAS.CODI_SAI
                    AND EFIMPSAI.CODI_IMP = 2
            ) AS TDIMPOSTO_IPI,
            LATERAL(
                SELECT MONTH(EFSAIDAS.DSAI_SAI) AS MES,
                    YEAR(EFSAIDAS.DSAI_SAI) AS ANO
                FROM DSDBA.DUMMY
            ) AS TDAUX
        WHERE EFSAIDAS.codi_emp = '#codi_emp#'
            and EFSAIDAS.DSAI_SAI >= DATE('#competence#')
            AND EFSAIDAS.DSAI_SAI <= DATE('#competence_fim#')
            and ( EFSAIDAS.DSAI_SAI >= par.SIMPLESN_INICIO_SISTEMA_PAR OR par.SIMPLESN_INICIO_SISTEMA_PAR is null )
            AND EFSAIDAS.CODI_NAT IN (5929, 6929)
            AND EFSAIDAS.NOVO_ECF = 'N'
            AND EFACUMULADOR_VIGENCIA.IFAT_ACU = 'N'
            AND EFACUMULADOR_VIGENCIA.VIGENCIA_ACU = (
                SELECT MAX(vig.VIGENCIA_ACU)
                FROM bethadba.EFACUMULADOR_VIGENCIA as vig
                WHERE vig.codi_emp = EFSAIDAS.codi_emp
                    and vig.codi_acu = EFSAIDAS.codi_acu
                    and vig.VIGENCIA_ACU <= EFSAIDAS.DSAI_SAI
            )
        GROUP BY TDAUX.MES,
            geempre.codi_emp,
            geempre.nome_emp,
            TDAUX.ANO,
            cgce
        UNION ALL
        SELECT SUM(TDAUX.VALOR_SAIDAS) AS VSAI,
            geempre.codi_emp,
            GEEMPRE.nome_emp,
            0 AS VIPI,
            0 AS VST,
            0 AS VSER,
            0 AS VOUT,
            TDAUX.MES AS MES,
            TDAUX.ANO AS ANO,
            saldo_imposto = 0,
            saldo_caixa = 0,
            entradas = 0,
            pis = 0,
            cofins = 0,
            csll = 0,
            irpj = 0,
            sn = 0,
            icms = 0,
            ipi = 0,
            cgce = geempre.cgce_emp
        FROM BETHADBA.EFEMPREENDIMENTOS_IMOBILIARIOS_CONTRATOS_VENDA_PAGAMENTOS AS PAGAMENTOS
            INNER JOIN BETHADBA.EFEMPREENDIMENTOS_IMOBILIARIOS_CONTRATOS_VENDA_PARCELAS AS PARCELAS ON PARCELAS.CODI_EMP = PAGAMENTOS.CODI_EMP
            AND PARCELAS.I_CONTRATO = PAGAMENTOS.I_CONTRATO
            AND PARCELAS.I_PARCELA = PAGAMENTOS.I_PARCELA
            INNER JOIN BETHADBA.EFEMPREENDIMENTOS_IMOBILIARIOS_CONTRATOS_VENDA AS CONTRATO_VENDA ON CONTRATO_VENDA.CODI_EMP = PARCELAS.CODI_EMP
            AND CONTRATO_VENDA.I_CONTRATO = PARCELAS.I_CONTRATO
            INNER JOIN BETHADBA.EFEMPREENDIMENTOS_IMOBILIARIOS AS EMPREENDIMENTO ON EMPREENDIMENTO.CODI_EMP = CONTRATO_VENDA.CODI_EMP
            AND EMPREENDIMENTO.I_EMPREENDIMENTO = CONTRATO_VENDA.I_EMPREENDIMENTO
            INNER JOIN BETHADBA.GEEMPRE AS GEEMPRE ON GEEMPRE.CODI_EMP = PAGAMENTOS.CODI_EMP
            inner join bethadba.efparametro as par 
                  on    par.codi_emp = geempre.codi_emp,
            LATERAL(
                SELECT MONTH(PAGAMENTOS.DATA_PAGAMENTO) AS MES,
                    YEAR(PAGAMENTOS.DATA_PAGAMENTO) AS ANO,
                    CASE
                        EMPREENDIMENTO.CALCULAR_PIS_COFINS_CORRECAO_MONETARIA
                        WHEN 'S' THEN PAGAMENTOS.VALOR_PAGO - PAGAMENTOS.VALOR_JUROS - PAGAMENTOS.VALOR_MULTA + PAGAMENTOS.VALOR_DEVOLUCAO
                        ELSE PAGAMENTOS.VALOR_PAGO - PAGAMENTOS.VALOR_CORRECAO - PAGAMENTOS.VALOR_JUROS - PAGAMENTOS.VALOR_MULTA + PAGAMENTOS.VALOR_DEVOLUCAO
                    END AS VALOR_SAIDAS
                FROM DSDBA.DUMMY
            ) AS TDAUX
        WHERE PAGAMENTOS.codi_emp = '#codi_emp#'
            and PAGAMENTOS.DATA_PAGAMENTO >= DATE('#competence#')
            AND PAGAMENTOS.DATA_PAGAMENTO <= DATE('#competence_fim#')
            and ( PAGAMENTOS.DATA_PAGAMENTO >= par.SIMPLESN_INICIO_SISTEMA_PAR OR par.SIMPLESN_INICIO_SISTEMA_PAR is null )
        GROUP BY TDAUX.MES,
            GEEMPRE.codi_emp,
            GEEMPRE.nome_emp,
            TDAUX.ANO,
            cgce
        UNION ALL
        SELECT - SUM(RESCISAO_CONTRATO_UNIDADES.VALOR_DEVOLVIDO) AS VSAI,
            geempre.codi_emp,
            GEEMPRE.nome_emp,
            0 AS VIPI,
            0 AS VST,
            0 AS VSER,
            0 AS VOUT,
            TDAUX.MES AS MES,
            TDAUX.ANO AS ANO,
            saldo_imposto = 0,
            saldo_caixa = 0,
            entradas = 0,
            pis = 0,
            cofins = 0,
            csll = 0,
            irpj = 0,
            sn = 0,
            icms = 0,
            ipi = 0,
            cgce = geempre.cgce_emp
        FROM BETHADBA.EFEMPREENDIMENTOS_IMOBILIARIOS_RESCISAO_CONTRATO AS RESCISAO_CONTRATO
            INNER JOIN BETHADBA.EFEMPREENDIMENTOS_IMOBILIARIOS_RESCISAO_CONTRATO_UNIDADES AS RESCISAO_CONTRATO_UNIDADES ON RESCISAO_CONTRATO_UNIDADES.CODI_EMP = RESCISAO_CONTRATO.CODI_EMP
            AND RESCISAO_CONTRATO_UNIDADES.I_RESCISAO = RESCISAO_CONTRATO.I_RESCISAO
            INNER JOIN BETHADBA.GEEMPRE AS GEEMPRE ON GEEMPRE.CODI_EMP = RESCISAO_CONTRATO.CODI_EMP
            inner join bethadba.efparametro as par 
                  on    par.codi_emp = geempre.codi_emp,
            LATERAL(
                SELECT MONTH(RESCISAO_CONTRATO.DATA_DEVOLUCAO) AS MES,
                    YEAR(RESCISAO_CONTRATO.DATA_DEVOLUCAO) AS ANO
                FROM DSDBA.DUMMY
            ) AS TDAUX
        WHERE RESCISAO_CONTRATO.codi_emp = '#codi_emp#'
            and RESCISAO_CONTRATO.DATA_DEVOLUCAO >= DATE('#competence#')
            AND RESCISAO_CONTRATO.DATA_DEVOLUCAO <= DATE('#competence_fim#')
            and ( RESCISAO_CONTRATO.DATA_DEVOLUCAO >= par.SIMPLESN_INICIO_SISTEMA_PAR OR par.SIMPLESN_INICIO_SISTEMA_PAR is null )
        GROUP BY TDAUX.MES,
            GEEMPRE.codi_emp,
            GEEMPRE.nome_emp,
            TDAUX.ANO,
            cgce
        UNION ALL
        /* entradas */
        SELECT 0 AS VSAI,
            geempre.codi_emp,
            GEEMPRE.nome_emp,
            0 AS VIPI,
            0 AS VST,
            0 AS VSER,
            0 AS VOUT,
            MONTH(ent.dent_ent) AS MES,
            YEAR(ent.dent_ent) AS ANO,
            saldo_imposto = 0,
            saldo_caixa = 0,
            entradas = sum(ent.vcon_ent),
            pis = 0,
            cofins = 0,
            csll = 0,
            irpj = 0,
            sn = 0,
            icms = 0,
            ipi = 0,
            cgce = geempre.cgce_emp
        FROM bethadba.geempre AS geempre
            inner join bethadba.efparametro as par 
                  on    par.codi_emp = geempre.codi_emp
            INNER JOIN bethadba.efentradas AS ent ON ent.codi_emp = geempre.codi_emp
            INNER JOIN bethadba.efacumulador_vigencia AS acuvig ON acuvig.codi_emp = ent.codi_emp
            AND acuvig.codi_acu = ent.codi_acu
        WHERE ent.codi_emp = '#codi_emp#'
            and ent.dent_ent BETWEEN DATE('#competence#') AND DATE('#competence_fim#')
            and ( ent.dent_ent >= par.SIMPLESN_INICIO_SISTEMA_PAR OR par.SIMPLESN_INICIO_SISTEMA_PAR is null )
            AND ent.codi_nat NOT IN (1933, 2933)
            AND acuvig.IFAT_ACU <> 'S'
            AND acuvig.IDEV_ACU <> 'S'
            AND acuvig.VIGENCIA_ACU = (
                SELECT max(acuvig2.VIGENCIA_ACU)
                FROM bethadba.EFACUMULADOR_VIGENCIA AS acuvig2
                WHERE acuvig2.codi_emp = acuvig.codi_emp
                    AND acuvig2.codi_acu = acuvig.codi_acu
                    AND acuvig2.vigencia_acu <= ent.dent_ent
            )
        GROUP BY MES,
            GEEMPRE.codi_emp,
            GEEMPRE.nome_emp,
            ANO,
            cgce
        UNION ALL
        /* entradas devolucao */
        SELECT 0 AS VSAI,
            geempre.codi_emp,
            GEEMPRE.nome_emp,
            0 AS VIPI,
            0 AS VST,
            0 AS VSER,
            0 AS VOUT,
            MONTH(sai.dsai_sai) AS MES,
            YEAR(sai.dsai_sai) AS ANO,
            saldo_imposto = 0,
            saldo_caixa = 0,
            entradas = - sum(sai.vcon_sai),
            pis = 0,
            cofins = 0,
            csll = 0,
            irpj = 0,
            sn = 0,
            icms = 0,
            ipi = 0,
            cgce = geempre.cgce_emp
        FROM bethadba.geempre AS geempre
            inner join bethadba.efparametro as par 
                  on    par.codi_emp = geempre.codi_emp
            INNER JOIN bethadba.efsaidas AS sai ON sai.codi_emp = geempre.codi_emp
            INNER JOIN bethadba.efacumulador_vigencia AS acuvig ON acuvig.codi_emp = sai.codi_emp
            AND acuvig.codi_acu = sai.codi_acu
        WHERE sai.codi_emp = '#codi_emp#'
            and sai.dsai_sai BETWEEN DATE('#competence#') AND DATE('#competence_fim#')
            and ( sai.dsai_sai >= par.SIMPLESN_INICIO_SISTEMA_PAR OR par.SIMPLESN_INICIO_SISTEMA_PAR is null )
            AND acuvig.IFAT_ACU <> 'S'
            AND acuvig.IDEV_ACU = 'S'
            AND acuvig.VIGENCIA_ACU = (
                SELECT max(acuvig2.VIGENCIA_ACU)
                FROM bethadba.EFACUMULADOR_VIGENCIA AS acuvig2
                WHERE acuvig2.codi_emp = acuvig.codi_emp
                    AND acuvig2.codi_acu = acuvig.codi_acu
                    AND acuvig2.vigencia_acu <= sai.dsai_sai
            )
        GROUP BY MES,
            GEEMPRE.codi_emp,
            GEEMPRE.nome_emp,
            ANO,
            cgce
        UNION ALL
        /* impostos */
        SELECT 0 AS VSAI,
            geempre.codi_emp,
            GEEMPRE.nome_emp,
            0 AS VIPI,
            0 AS VST,
            0 AS VSER,
            0 AS VOUT,
            MONTH(simp.data_sim) AS MES,
            YEAR(simp.data_sim) AS ANO,
            saldo_imposto = 0,
            saldo_caixa = 0,
            entradas = 0,
            pis = SUM(
                CASE
                    WHEN simp.codi_imp IN (4, 17) THEN simp.sdev_sim
                    ELSE 0
                END
            ),
            cofins = SUM(
                CASE
                    WHEN simp.codi_imp IN (5, 19) THEN simp.sdev_sim
                    ELSE 0
                END
            ),
            csll = SUM(
                CASE
                    WHEN simp.codi_imp IN (6) THEN simp.sdev_sim
                    ELSE 0
                END
            ),
            irpj = SUM(
                CASE
                    WHEN simp.codi_imp IN (7) THEN simp.sdev_sim
                    ELSE 0
                END
            ),
            sn = SUM(
                CASE
                    WHEN simp.codi_imp IN (44) THEN simp.sdev_sim
                    ELSE 0
                END
            ),
            icms = SUM(
                CASE
                    WHEN simp.codi_imp IN (1) THEN simp.sdev_sim
                    ELSE 0
                END
            ),
            ipi = SUM(
                CASE
                    WHEN simp.codi_imp IN (2) THEN simp.sdev_sim
                    ELSE 0
                END
            ),
            cgce = geempre.cgce_emp
        FROM bethadba.geempre AS geempre
            inner join bethadba.efparametro as par 
                  on    par.codi_emp = geempre.codi_emp
            INNER JOIN bethadba.efsdoimp AS simp ON simp.codi_emp = geempre.codi_emp
            INNER JOIN bethadba.geimposto AS imp ON imp.codi_emp = simp.codi_emp
            AND imp.codi_imp = simp.codi_imp
            INNER JOIN bethadba.geimposto_vigencia AS impvig ON impvig.codi_emp = imp.codi_emp
            AND impvig.codi_imp = imp.codi_imp
        WHERE simp.codi_emp = '#codi_emp#'
            and simp.data_sim BETWEEN DATE('#competence#') AND DATE('#competence_fim#')
            and ( simp.data_sim >= par.SIMPLESN_INICIO_SISTEMA_PAR OR par.SIMPLESN_INICIO_SISTEMA_PAR is null )
            AND impvig.vigencia_imp = (
                SELECT MAX(impvig2.vigencia_imp)
                FROM bethadba.geimposto_vigencia AS impvig2
                WHERE impvig2.codi_emp = impvig.codi_emp
                    AND impvig2.codi_imp = impvig.codi_imp
                    AND impvig2.vigencia_imp <= simp.data_sim
            )
            AND NOT EXISTS (
                SELECT 1
                FROM bethadba.efsdoimp_parcelas AS simppar
                WHERE simppar.codi_emp = simp.codi_emp
                    AND simppar.codi_imp = simp.codi_imp
                    AND simppar.data_sim = simp.data_sim
                    AND simppar.pdic_sim = simp.pdic_sim
            )
        GROUP BY MES,
            GEEMPRE.codi_emp,
            GEEMPRE.nome_emp,
            ANO,
            cgce
        
        /* receita bruta simples nacional periodo anterior */
       union all

       SELECT SUM(REC_BRUT_ACUM.valor) AS VSAI,
		      geempre.codi_emp,
              GEEMPRE.nome_emp,
              0 AS VIPI,
              0 AS VST,
              0 AS VSER,
              0 AS VOUT,
              MONTH(REC_BRUT_ACUM.periodo) AS MES,
              YEAR(REC_BRUT_ACUM.periodo) AS ANO,
              saldo_imposto = 0,
              saldo_caixa = 0,
              entradas = 0,
              pis = 0,
              cofins = 0,
              csll = 0,
              irpj = 0,
              sn = 0,
              icms = 0,
              ipi = 0,
              cgce = geempre.cgce_emp

		FROM bethadba.efsimples_nacional_receita_bruta_anterior AS REC_BRUT_ACUM 
             INNER JOIN BETHADBA.GEEMPRE AS GEEMPRE
				     ON REC_BRUT_ACUM.CODI_EMP = GEEMPRE.CODI_EMP
             inner join bethadba.efparametro as par 
                  on    par.codi_emp = geempre.codi_emp
						
	  WHERE REC_BRUT_ACUM.codi_emp = '#codi_emp#'
        and REC_BRUT_ACUM.periodo BETWEEN DATE('#competence#') AND DATE('#competence_fim#')
        and REC_BRUT_ACUM.periodo < par.SIMPLESN_INICIO_SISTEMA_PAR
            
	 GROUP BY MES,
            GEEMPRE.codi_emp,
            GEEMPRE.nome_emp,
            ANO,
            cgce

  ) AS TD_DADOS
GROUP BY TD_DADOS.codi_emp,
    td_dados.nome_emp,
    td_dados.cgce,
     td_dados.mes,
     td_dados.ano
order by ano, mes