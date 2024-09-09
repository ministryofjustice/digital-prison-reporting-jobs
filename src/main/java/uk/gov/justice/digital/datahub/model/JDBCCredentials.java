package uk.gov.justice.digital.datahub.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class JDBCCredentials {
    private String username;
    private String password;
}
