class HtmlConverter:

    def __init__(self, logs, option, fields_to_update, start_time, end_time, initial_date, final_date, companies):
        times_last = [logs[id_q]["CIQ"]["quarter"]["Time Last Update"] for id_q in logs.keys()]
        times_request = [logs[id_q]["CIQ"]["quarter"]["Time CIQ request"] for id_q in logs.keys()]
        times_dump = [logs[id_q]["CIQ"]["quarter"]["Time Dump"] for id_q in logs.keys()]
        self.logs = logs
        self.avg_times_last = sum(times_last)/len(times_last)
        self.avg_times_request = sum(times_request)/len(times_request)
        self.avg_times_dump = sum(times_dump)/len(times_dump)
        self.a_fields = len(fields_to_update)
        self.option = option
        self.total_time = (end_time - start_time)/60
        self.intial_date = initial_date
        self.final_date = final_date
        self.companies = companies
        return

    def calculate_table(self):
        fields_info = [ {"id" : id_q, "fields": self.logs[id_q]["Updated fields"]} for id_q in self.logs.keys() ]
        fields_info.sort(key = lambda f : f["fields"])
        self.fields_info = fields_info
        self.fields_html = ""
        companies = self.companies
        for companie in self.fields_info:
            self.fields_html += f"""<tr>
                                        <td> {companie['id']} </td>
                                        <td> {companies.loc[companie['id']]["Ticker Bloomberg"]} </td>
                                        <td> {companies.loc[companie['id']].Industry_Sector} </td>
                                        <td> {companie['fields']} </td>
                                    </tr>"""
        return

    def get_print(self):    
        self.calculate_table()
        mail_msg = f"Se a consultado por {self.a_fields} campos"
        mail_msg += f"Los tiempos promedios de ejecución corresponden a:\n"
        mail_msg += f"- Tiempo prom. de consulta ultimo registro: {self.avg_times_last}\n"
        mail_msg += f"- Tiempo prom. de consulta a CIQ: {self.avg_times_request}\n"
        mail_msg += f"- Tiempo prom de escritura de archivos: {self.avg_times_dump}\n"
        mail_msg += f"Esto repercute en un tiempo total de ejecución de: {self.total_time} minutos\n"
        for companie in self.fields_info:
            mail_msg += f"{companie['id']} | {companie['fields']}\n"
        return mail_msg

    def get_html(self):
        self.calculate_table
        html = f"""\
        <html>
        <body>
            <p>Hola,<br>
                Este email se ah generado automáticamente como reporte de la actualización de base de datos mongo
                ocurrida entre {self.intial_date} y {self.final_date} (UTC). A continuación se encuentra un resumen del 
                log adjunto. <br>
                Se ha consultado por {self.a_fields} campos, con periodicidad <b> {self.option} </b>
                Los tiempos promedios de ejecución corresponden a:<br> 
                <ul>    
                    <li> Tiempo prom. de consulta ultimo registro: {self.avg_times_last} </li>
                    <li> Tiempo prom. de consulta a CIQ: {self.avg_times_request} </li>
                    <li> Tiempo prom de escritura de archivos: {self.avg_times_dump} </li>
                </ul>
                Esto repercute en un tiempo total de ejecución de: {self.total_time} minutos. <br>
                Al final de este email se encuentra una tabla que indicá cuantos campos fuerón actualizados correctamente 
                para cada Equity. <br>
            </p>

            <p>
                Saludos, <br>
                El mejor practicante.  
            </p>

            <table>
                <tr>
                    <th> ID Quant </th>
                    <th> Ticker Bloomberg </th> 
                    <th> Industry Sector </th> 
                    <th> Campos Actualizados </th>
                </tr>
                {self.fields_html}
            </table>
        </body>
        </html>
        """
        return html
